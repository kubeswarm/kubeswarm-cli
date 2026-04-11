/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package swarm

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	sigsyaml "sigs.k8s.io/yaml"
)

const kindSwarmAgent = "SwarmAgent"
const kindSwarmTeam = "SwarmTeam"

// FileRefConfigMap describes a ConfigMap to be created for a fileRef resolution.
// Used by swarm deploy to materialise fileRef prompts as cluster objects.
type FileRefConfigMap struct {
	Name        string // e.g. "my-team-researcher-prompt"
	Key         string // data key inside the ConfigMap — always "prompt"
	Content     string // full file content
	SourcePath  string // absolute path of the source file (for annotation)
	ContentHash string // "sha256:" + first 16 hex chars (for change detection)
}

// ToYAML returns a serialised ConfigMap manifest ready to pipe to kubectl apply.
func (f FileRefConfigMap) ToYAML() []byte {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name": f.Name,
			"annotations": map[string]any{
				"swarm.kubeswarm.io/source-file":  f.SourcePath,
				"swarm.kubeswarm.io/content-hash": f.ContentHash,
			},
		},
		"data": map[string]any{
			f.Key: f.Content,
		},
	}
	out, _ := sigsyaml.Marshal(obj)
	return out
}

// InlineFileRefs walks raw multi-document YAML and resolves every
// systemPromptRef.fileRef.path relative to baseDir. The file content
// replaces the systemPromptRef block as an inline systemPrompt field.
//
// Used by swarm run and swarm validate — no ConfigMap or cluster required.
// If any referenced file does not exist, an error is returned immediately.
func InlineFileRefs(data []byte, baseDir string) ([]byte, error) {
	return resolveAllDocs(data, baseDir, false)
}

// ExtractFileRefs walks raw multi-document YAML and resolves every
// systemPromptRef.fileRef.path relative to baseDir. Each fileRef is
// rewritten to a configMapKeyRef pointing at a generated ConfigMap.
//
// Returns the rewritten YAML and the ConfigMap descriptors; callers
// must apply the ConfigMaps before applying the rewritten resources.
// Used by swarm deploy.
func ExtractFileRefs(data []byte, baseDir string) ([]byte, []FileRefConfigMap, error) {
	docs := splitDocs(data)
	var outDocs [][]byte
	var allCMs []FileRefConfigMap

	for _, doc := range docs {
		out, cms, err := resolveDoc(doc, baseDir, true)
		if err != nil {
			return nil, nil, err
		}
		outDocs = append(outDocs, out)
		allCMs = append(allCMs, cms...)
	}
	return joinDocs(outDocs), allCMs, nil
}

// resolveAllDocs is the multi-doc wrapper for the inline (non-extract) path.
func resolveAllDocs(data []byte, baseDir string, extract bool) ([]byte, error) {
	docs := splitDocs(data)
	out := make([][]byte, 0, len(docs))
	for _, doc := range docs {
		resolved, _, err := resolveDoc(doc, baseDir, extract)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return joinDocs(out), nil
}

// resolveDoc processes a single YAML document. If the document kind is
// SwarmTeam or SwarmAgent and contains fileRef entries, they are resolved.
// Other document kinds are returned unchanged without re-marshalling.
func resolveDoc(doc []byte, baseDir string, extract bool) ([]byte, []FileRefConfigMap, error) {
	// Peek at kind without a full unmarshal to short-circuit non-Swarm docs.
	var meta struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := sigsyaml.Unmarshal(doc, &meta); err != nil || (meta.Kind != kindSwarmTeam && meta.Kind != kindSwarmAgent) {
		return doc, nil, nil
	}

	var obj map[string]any
	if err := sigsyaml.Unmarshal(doc, &obj); err != nil {
		return doc, nil, nil
	}

	resourceName := meta.Metadata.Name
	var cms []FileRefConfigMap

	switch meta.Kind {
	case kindSwarmTeam:
		spec, _ := obj["spec"].(map[string]any)
		if spec == nil {
			break
		}
		roles, _ := spec["roles"].([]any)
		for _, r := range roles {
			role, ok := r.(map[string]any)
			if !ok {
				continue
			}
			roleName, _ := role["name"].(string)
			cmName := configMapName(resourceName, roleName)
			collected, err := processFileRef(role, cmName, baseDir, extract)
			if err != nil {
				return nil, nil, fmt.Errorf("SwarmTeam %q role %q: %w", resourceName, roleName, err)
			}
			cms = append(cms, collected...)
		}

	case kindSwarmAgent:
		spec, _ := obj["spec"].(map[string]any)
		if spec == nil {
			break
		}
		// SwarmAgent uses spec.prompt.from (new API) or spec.systemPromptRef (team role).
		// Check both paths for fileRef resolution.
		cmName := configMapName(resourceName, "")
		// New API path: spec.prompt.from.fileRef
		if prompt, ok := spec["prompt"].(map[string]any); ok {
			if from, ok := prompt["from"].(map[string]any); ok {
				collected, err := processPromptFromFileRef(from, prompt, cmName, baseDir, extract)
				if err != nil {
					return nil, nil, fmt.Errorf("SwarmAgent %q: %w", resourceName, err)
				}
				cms = append(cms, collected...)
			}
		}
		// Legacy path: spec.systemPromptRef (for SwarmTeam role definitions)
		collected, err := processFileRef(spec, cmName, baseDir, extract)
		if err != nil {
			return nil, nil, fmt.Errorf("SwarmAgent %q: %w", resourceName, err)
		}
		cms = append(cms, collected...)
	}

	out, err := sigsyaml.Marshal(obj)
	if err != nil {
		return nil, nil, err
	}
	return out, cms, nil
}

// processFileRef checks parent for a systemPromptRef.fileRef entry.
// If found, resolves the file and either inlines the content (extract=false)
// or rewrites to configMapKeyRef and returns a FileRefConfigMap (extract=true).
// parent is mutated in place (map is a reference type).
func processFileRef(parent map[string]any, cmName, baseDir string, extract bool) ([]FileRefConfigMap, error) {
	spr, ok := parent["systemPromptRef"].(map[string]any)
	if !ok {
		return nil, nil
	}
	fileRefMap, ok := spr["fileRef"].(map[string]any)
	if !ok {
		return nil, nil // has systemPromptRef but not a fileRef — leave it alone
	}
	rawPath, _ := fileRefMap["path"].(string)
	if rawPath == "" {
		return nil, fmt.Errorf("fileRef.path is empty")
	}

	absPath := filepath.Join(baseDir, rawPath)
	contentBytes, err := os.ReadFile(absPath) //nolint:gosec // path is resolved relative to a user-provided base dir; intentional
	if err != nil {
		return nil, fmt.Errorf("reading fileRef %q: %w", rawPath, err)
	}
	content := string(contentBytes)

	if extract {
		hash := fileContentHash(content)
		delete(parent, "systemPromptRef")
		parent["systemPromptRef"] = map[string]any{
			"configMapKeyRef": map[string]any{
				"name": cmName,
				"key":  "prompt",
			},
		}
		return []FileRefConfigMap{{
			Name:        cmName,
			Key:         "prompt",
			Content:     content,
			SourcePath:  absPath,
			ContentHash: hash,
		}}, nil
	}

	// Inline mode.
	delete(parent, "systemPromptRef")
	parent["systemPrompt"] = content
	return nil, nil
}

// processPromptFromFileRef handles spec.prompt.from.fileRef for SwarmAgent (new API).
// If found, resolves the file and either inlines (extract=false) or rewrites to
// configMapKeyRef (extract=true). The prompt map is mutated in place.
func processPromptFromFileRef(from, prompt map[string]any, cmName, baseDir string, extract bool) ([]FileRefConfigMap, error) {
	fileRefMap, ok := from["fileRef"].(map[string]any)
	if !ok {
		return nil, nil
	}
	rawPath, _ := fileRefMap["path"].(string)
	if rawPath == "" {
		return nil, fmt.Errorf("fileRef.path is empty")
	}

	absPath := filepath.Join(baseDir, rawPath)
	contentBytes, err := os.ReadFile(absPath) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("reading fileRef %q: %w", rawPath, err)
	}
	content := string(contentBytes)

	if extract {
		hash := fileContentHash(content)
		delete(from, "fileRef")
		from["configMapKeyRef"] = map[string]any{
			"name": cmName,
			"key":  "prompt",
		}
		return []FileRefConfigMap{{
			Name:        cmName,
			Key:         "prompt",
			Content:     content,
			SourcePath:  absPath,
			ContentHash: hash,
		}}, nil
	}

	// Inline mode: replace prompt.from with prompt.inline.
	delete(prompt, "from")
	prompt["inline"] = content
	return nil, nil
}

// configMapName builds the ConfigMap name for a fileRef.
// roleName is empty for SwarmAgent (no role level).
func configMapName(resourceName, roleName string) string {
	if roleName == "" {
		return resourceName + "-prompt"
	}
	return resourceName + "-" + roleName + "-prompt"
}

// fileContentHash returns "sha256:" + first 16 hex chars of the SHA-256 hash.
func fileContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "sha256:" + fmt.Sprintf("%x", sum[:])[:16]
}

// joinDocs joins YAML document byte slices with --- separators.
func joinDocs(docs [][]byte) []byte {
	var result []byte
	for i, doc := range docs {
		if i > 0 {
			result = append(result, []byte("\n---\n")...)
		}
		result = append(result, doc...)
	}
	return result
}

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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInlineFileRefs_SwarmTeamRole(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "researcher.md"), []byte("You are a research agent."), 0600); err != nil {
		t.Fatal(err)
	}

	yaml := []byte(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmTeam
metadata:
  name: my-team
spec:
  roles:
    - name: researcher
      model: claude-sonnet-4-20250514
      systemPromptRef:
        fileRef:
          path: ./researcher.md
`)

	out, err := InlineFileRefs(yaml, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "systemPrompt: You are a research agent.") {
		t.Errorf("expected inlined systemPrompt, got:\n%s", s)
	}
	if strings.Contains(s, "systemPromptRef") {
		t.Errorf("systemPromptRef should be removed after inlining, got:\n%s", s)
	}
	if strings.Contains(s, "fileRef") {
		t.Errorf("fileRef should be removed after inlining, got:\n%s", s)
	}
}

func TestInlineFileRefs_SwarmAgent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "agent.txt"), []byte("You are an agent."), 0600); err != nil {
		t.Fatal(err)
	}

	yaml := []byte(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmAgent
metadata:
  name: my-agent
spec:
  model: gpt-4o
  systemPromptRef:
    fileRef:
      path: ./agent.txt
`)

	out, err := InlineFileRefs(yaml, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "systemPrompt: You are an agent.") {
		t.Errorf("expected inlined systemPrompt, got:\n%s", s)
	}
	if strings.Contains(s, "fileRef") {
		t.Errorf("fileRef should be removed, got:\n%s", s)
	}
}

func TestInlineFileRefs_NoFileRef_Passthrough(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmTeam
metadata:
  name: my-team
spec:
  roles:
    - name: researcher
      model: claude-sonnet-4-20250514
      systemPrompt: |
        You are a research agent.
`)
	out, err := InlineFileRefs(yaml, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(out), "You are a research agent.") {
		t.Errorf("inline systemPrompt should be preserved, got:\n%s", string(out))
	}
}

func TestInlineFileRefs_MissingFile_Error(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmTeam
metadata:
  name: my-team
spec:
  roles:
    - name: researcher
      model: claude-sonnet-4-20250514
      systemPromptRef:
        fileRef:
          path: ./does-not-exist.md
`)
	_, err := InlineFileRefs(yaml, dir)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist.md") {
		t.Errorf("error should mention the missing file, got: %v", err)
	}
}

func TestExtractFileRefs_SwarmTeamRole(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "researcher.md"), []byte("You are a research agent."), 0600); err != nil {
		t.Fatal(err)
	}

	yaml := []byte(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmTeam
metadata:
  name: my-team
spec:
  roles:
    - name: researcher
      model: claude-sonnet-4-20250514
      systemPromptRef:
        fileRef:
          path: ./researcher.md
`)

	rewritten, cms, err := ExtractFileRefs(yaml, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(cms) != 1 {
		t.Fatalf("expected 1 ConfigMap, got %d", len(cms))
	}
	cm := cms[0]
	if cm.Name != "my-team-researcher-prompt" {
		t.Errorf("expected ConfigMap name %q, got %q", "my-team-researcher-prompt", cm.Name)
	}
	if cm.Key != "prompt" {
		t.Errorf("expected key %q, got %q", "prompt", cm.Key)
	}
	if cm.Content != "You are a research agent." {
		t.Errorf("unexpected content: %q", cm.Content)
	}
	if !strings.HasPrefix(cm.ContentHash, "sha256:") {
		t.Errorf("unexpected content hash: %q", cm.ContentHash)
	}

	s := string(rewritten)
	if !strings.Contains(s, "configMapKeyRef") {
		t.Errorf("rewritten YAML should contain configMapKeyRef, got:\n%s", s)
	}
	if !strings.Contains(s, "my-team-researcher-prompt") {
		t.Errorf("rewritten YAML should reference the ConfigMap name, got:\n%s", s)
	}
	if strings.Contains(s, "fileRef") {
		t.Errorf("rewritten YAML should not contain fileRef, got:\n%s", s)
	}

	cmYAML := string(cm.ToYAML())
	if !strings.Contains(cmYAML, "kind: ConfigMap") {
		t.Errorf("ToYAML should produce a ConfigMap, got:\n%s", cmYAML)
	}
	if !strings.Contains(cmYAML, "swarm.kubeswarm.io/content-hash") {
		t.Errorf("ToYAML should include content-hash annotation, got:\n%s", cmYAML)
	}
}

func TestInlineFileRefs_NonSwarmDoc_Unchanged(t *testing.T) {
	dir := t.TempDir()
	yaml := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: my-config
data:
  key: value
`)
	out, err := InlineFileRefs(yaml, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Non-Swarm docs pass through without re-marshalling (whitespace is preserved).
	if !strings.Contains(string(out), "name: my-config") {
		t.Errorf("non-Swarm doc content should be preserved, got:\n%s", string(out))
	}
	if strings.Contains(string(out), "fileRef") {
		t.Errorf("non-Swarm doc should not have fileRef injected")
	}
}

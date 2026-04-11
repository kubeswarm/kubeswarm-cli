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

// Package swarm provides shared utilities for the swarm CLI.
package swarm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	sigsyaml "sigs.k8s.io/yaml"

	swarmv1alpha1 "github.com/kubeswarm/kubeswarm/api/v1alpha1"
)

// LoadFile reads a multi-document YAML file and returns all SwarmTeam (pipeline mode)
// and SwarmAgent resources it contains.
// Unknown kinds are silently skipped — this matches the behaviour of kubectl apply -f.
func LoadFile(path string) ([]*swarmv1alpha1.SwarmTeam, map[string]*swarmv1alpha1.SwarmAgent, error) {
	data, err := os.ReadFile(path) //nolint:gosec // CLI reads a user-specified file path; intentional
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s: %w", path, err)
	}
	data, err = InlineFileRefs(data, filepath.Dir(path))
	if err != nil {
		return nil, nil, fmt.Errorf("resolving fileRefs in %s: %w", path, err)
	}
	return ParseDocs(data)
}

// ParseDocs parses raw multi-document YAML bytes into SwarmTeam and SwarmAgent objects.
// Exported so callers can parse in-memory YAML without a file (e.g. tests).
func ParseDocs(data []byte) ([]*swarmv1alpha1.SwarmTeam, map[string]*swarmv1alpha1.SwarmAgent, error) {
	var teams []*swarmv1alpha1.SwarmTeam
	agents := make(map[string]*swarmv1alpha1.SwarmAgent)

	for i, part := range splitDocs(data) {
		var meta struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := sigsyaml.Unmarshal(part, &meta); err != nil || meta.Kind == "" {
			continue
		}

		switch meta.Kind {
		case "SwarmTeam":
			var t swarmv1alpha1.SwarmTeam
			if err := sigsyaml.Unmarshal(part, &t); err != nil {
				return nil, nil, fmt.Errorf("document %d (SwarmTeam): %w", i, err)
			}
			teams = append(teams, &t)

		case "SwarmAgent":
			var a swarmv1alpha1.SwarmAgent
			if err := sigsyaml.Unmarshal(part, &a); err != nil {
				return nil, nil, fmt.Errorf("document %d (SwarmAgent): %w", i, err)
			}
			agents[a.Name] = &a
		}
	}

	return teams, agents, nil
}

// RawDoc is a single YAML document with its kind field extracted.
// Used by swarm deploy to sort resources before applying.
type RawDoc struct {
	Kind string
	Raw  []byte
}

// SplitRawDocs splits multi-document YAML into individual documents, extracting
// the kind field from each so callers can sort before applying.
func SplitRawDocs(data []byte) []RawDoc {
	parts := splitDocs(data)
	docs := make([]RawDoc, 0, len(parts))
	for _, part := range parts {
		var meta struct {
			Kind string `json:"kind"`
		}
		_ = sigsyaml.Unmarshal(part, &meta)
		docs = append(docs, RawDoc{Kind: meta.Kind, Raw: part})
	}
	return docs
}

// splitDocs splits a YAML byte slice on --- document separators.
func splitDocs(data []byte) [][]byte {
	var docs [][]byte
	for part := range bytes.SplitSeq(data, []byte("\n---")) {
		part = bytes.TrimPrefix(bytes.TrimSpace(part), []byte("---"))
		part = bytes.TrimSpace(part)
		if len(part) > 0 {
			docs = append(docs, part)
		}
	}
	return docs
}

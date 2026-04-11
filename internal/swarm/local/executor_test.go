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

package local_test

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeswarm/kubeswarm-cli/internal/swarm/local"
	swarmv1alpha1 "github.com/kubeswarm/kubeswarm/api/v1alpha1"
	"github.com/kubeswarm/kubeswarm/pkg/agent/providers"
	"github.com/kubeswarm/kubeswarm/pkg/agent/providers/mock"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeAgent(name, model, prompt string) *swarmv1alpha1.SwarmAgent {
	replicas := int32(1)
	return &swarmv1alpha1.SwarmAgent{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: swarmv1alpha1.SwarmAgentSpec{
			Model:   model,
			Prompt:  &swarmv1alpha1.AgentPrompt{Inline: prompt},
			Runtime: swarmv1alpha1.AgentRuntime{Replicas: &replicas},
		},
	}
}

// mockExecutor builds an Executor that always returns the given response for any model.
func mockExecutor(response string) *local.Executor {
	p := &mock.Provider{Default: response}
	return &local.Executor{
		Provider: func(_ string) (providers.LLMProvider, error) { return p, nil },
		NoMCP:    true,
	}
}

// errExecutor builds an Executor whose provider always returns an error.
func errExecutor(msg string) *local.Executor {
	return &local.Executor{
		Provider: func(_ string) (providers.LLMProvider, error) {
			return nil, fmt.Errorf("%s", msg)
		},
		NoMCP: true,
	}
}

// collectTeamEvents runs RunTeam and returns all emitted events.
func collectTeamEvents(
	t *testing.T,
	ex *local.Executor,
	team *swarmv1alpha1.SwarmTeam,
	agents map[string]*swarmv1alpha1.SwarmAgent,
) ([]local.StepEvent, *swarmv1alpha1.SwarmRun) {
	t.Helper()
	var events []local.StepEvent
	run, err := ex.RunTeam(context.Background(), team, agents, func(e local.StepEvent) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("RunTeam returned unexpected error: %v", err)
	}
	return events, run
}

// makeTeam builds a minimal SwarmTeam with the given pipeline steps (all referencing the same agent).
func makeTeam(agentName string, roles ...string) *swarmv1alpha1.SwarmTeam {
	team := &swarmv1alpha1.SwarmTeam{
		ObjectMeta: metav1.ObjectMeta{Name: "test-team"},
	}
	for _, role := range roles {
		team.Spec.Roles = append(team.Spec.Roles, swarmv1alpha1.SwarmTeamRole{
			Name:       role,
			SwarmAgent: agentName,
		})
		team.Spec.Pipeline = append(team.Spec.Pipeline, swarmv1alpha1.SwarmTeamPipelineStep{
			Role:   role,
			Inputs: map[string]string{"prompt": "do work for " + role},
		})
	}
	return team
}

// makeTeamChain builds a two-step SwarmTeam where step B depends on step A.
func makeTeamChain(agentA, agentB string) *swarmv1alpha1.SwarmTeam {
	team := &swarmv1alpha1.SwarmTeam{
		ObjectMeta: metav1.ObjectMeta{Name: "test-team"},
		Spec: swarmv1alpha1.SwarmTeamSpec{
			Roles: []swarmv1alpha1.SwarmTeamRole{
				{Name: "step-a", SwarmAgent: agentA},
				{Name: "step-b", SwarmAgent: agentB},
			},
			Pipeline: []swarmv1alpha1.SwarmTeamPipelineStep{
				{Role: "step-a", Inputs: map[string]string{"prompt": "do a"}},
				{Role: "step-b", DependsOn: []string{"step-a"}, Inputs: map[string]string{"prompt": "do b"}},
			},
		},
	}
	return team
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRunTeam_HappyPath(t *testing.T) {
	agent := makeAgent("agent", "mock", "You are a researcher.")
	team := makeTeam("agent", "research")
	agents := map[string]*swarmv1alpha1.SwarmAgent{"agent": agent}

	ex := mockExecutor("research complete")
	events, run := collectTeamEvents(t, ex, team, agents)

	if run.Status.Phase != swarmv1alpha1.SwarmRunPhaseSucceeded {
		t.Errorf("expected Succeeded, got %q", run.Status.Phase)
	}

	found := false
	for _, e := range events {
		if e.Step == "research" && e.Phase == swarmv1alpha1.PipelineStepPhaseSucceeded {
			found = true
			if e.Output != "research complete" {
				t.Errorf("expected output %q, got %q", "research complete", e.Output)
			}
		}
	}
	if !found {
		t.Error("no Succeeded event emitted for 'research' step")
	}
}

func TestRunTeam_TwoStepChain(t *testing.T) {
	agents := map[string]*swarmv1alpha1.SwarmAgent{
		"agent-a": makeAgent("agent-a", "mock", "step a"),
		"agent-b": makeAgent("agent-b", "mock", "step b"),
	}
	team := makeTeamChain("agent-a", "agent-b")

	_, run := collectTeamEvents(t, mockExecutor("done"), team, agents)

	if run.Status.Phase != swarmv1alpha1.SwarmRunPhaseSucceeded {
		t.Errorf("expected Succeeded, got %q", run.Status.Phase)
	}
	for _, st := range run.Status.Steps {
		if st.Phase != swarmv1alpha1.PipelineStepPhaseSucceeded {
			t.Errorf("step %q: expected Succeeded, got %q", st.Name, st.Phase)
		}
	}
}

func TestRunTeam_ProviderError_StepFails(t *testing.T) {
	agent := makeAgent("agent", "bad-model", "prompt")
	team := makeTeam("agent", "step1")
	agents := map[string]*swarmv1alpha1.SwarmAgent{"agent": agent}

	_, run := collectTeamEvents(t, errExecutor("unsupported model"), team, agents)

	if run.Status.Phase != swarmv1alpha1.SwarmRunPhaseFailed {
		t.Errorf("expected Failed on provider error, got %q", run.Status.Phase)
	}
}

func TestRunTeam_NoPipeline_ReturnsError(t *testing.T) {
	team := &swarmv1alpha1.SwarmTeam{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic-team"},
		// No pipeline — dynamic mode only.
	}
	ex := mockExecutor("ok")
	_, err := ex.RunTeam(context.Background(), team, nil, func(local.StepEvent) {})
	if err == nil {
		t.Error("expected error for team with no pipeline")
	}
}

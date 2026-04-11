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

// Package local provides a pipeline executor that runs pipeline steps directly
// in the current process - no Kubernetes cluster or Redis required.
// It reuses runtime/agent's Runner (MCP, tool dispatch, provider loop)
// and the pipeline DAG logic unchanged.
package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	swarmv1alpha1 "github.com/kubeswarm/kubeswarm/api/v1alpha1"
	"github.com/kubeswarm/kubeswarm/pkg/agent/config"
	"github.com/kubeswarm/kubeswarm/pkg/agent/mcp"
	"github.com/kubeswarm/kubeswarm/pkg/agent/providers"
	"github.com/kubeswarm/kubeswarm/pkg/agent/queue"
	"github.com/kubeswarm/kubeswarm/pkg/agent/runner"
	"github.com/kubeswarm/kubeswarm/pkg/artifacts"
	"github.com/kubeswarm/kubeswarm/pkg/costs"
	"github.com/kubeswarm/kubeswarm/pkg/flow"
)

// StepEvent is emitted by the executor on each step state transition.
// Callers use it to render --watch output or collect --output json results.
type StepEvent struct {
	Step    string
	Phase   swarmv1alpha1.PipelineStepPhase
	Output  string
	Tokens  queue.TokenUsage
	CostUSD float64
	Elapsed time.Duration
	Err     error
	// Validated is true when a validate block was configured and all checks passed.
	Validated bool
	// RetryCount is the current validation retry attempt number (> 0 on retry events).
	RetryCount int
}

// Executor drives SwarmTeam pipelines locally without a Kubernetes cluster or Redis.
type Executor struct {
	// Provider returns the LLMProvider to use for a given model name.
	// Each step calls Provider(agent.Spec.Model), so a flow can mix models
	// from different backends (e.g. claude-* and gpt-*) in the same run.
	Provider func(model string) (providers.LLMProvider, error)

	// NoMCP disables all MCP tool connections. When false (the default),
	// unreachable MCP servers are silently skipped.
	NoMCP bool

	// SemanticValidateFn makes a direct single-turn LLM call for semantic validation.
	// When nil, semantic validation is skipped with a warning printed to stderr.
	SemanticValidateFn func(ctx context.Context, model, prompt string) (string, error)

	// CostProvider is used to estimate USD cost for each step's token usage.
	// When nil, CostUSD fields are left at zero.
	CostProvider costs.CostProvider

	// ArtifactStore is the backend where step-produced file artifacts are stored.
	// When nil, artifact declarations on pipeline steps are silently ignored.
	ArtifactStore artifacts.Store

	// ArtifactBaseDir is the local base directory scanned for artifacts after each step.
	// Each step writes files to <ArtifactBaseDir>/<stepName>/<artifactName>.
	// When empty and ArtifactStore is set, a temporary directory is created automatically.
	ArtifactBaseDir string
}

// RunTeam executes an SwarmTeam pipeline to completion, calling onEvent on each step transition.
// Only pipeline mode (spec.pipeline set) is supported locally. Dynamic mode requires a cluster.
// Returns the SwarmRun execution record populated with step statuses and final output.
func (e *Executor) RunTeam(
	ctx context.Context,
	t *swarmv1alpha1.SwarmTeam,
	agents map[string]*swarmv1alpha1.SwarmAgent,
	onEvent func(StepEvent),
) (*swarmv1alpha1.SwarmRun, error) {
	if len(t.Spec.Pipeline) == 0 {
		return nil, fmt.Errorf("team %q has no pipeline — dynamic mode requires a Kubernetes cluster", t.Name)
	}

	// Create an in-memory SwarmRun from the team spec for this local execution.
	run := &swarmv1alpha1.SwarmRun{
		Spec: swarmv1alpha1.SwarmRunSpec{
			TeamRef:        t.Name,
			Pipeline:       t.Spec.Pipeline,
			Roles:          t.Spec.Roles,
			Input:          t.Spec.Input,
			Output:         t.Spec.Output,
			TimeoutSeconds: t.Spec.TimeoutSeconds,
			MaxTokens:      t.Spec.MaxTokens,
		},
	}

	// Build agent lookup: for each pipeline step, find the SwarmAgent.
	// Inline roles (model+systemPrompt set) use auto-generated SwarmAgent named "{team}-{role}".
	resolveAgent := func(roleName string) (*swarmv1alpha1.SwarmAgent, error) {
		for _, role := range t.Spec.Roles {
			if role.Name == roleName {
				if role.SwarmAgent != "" {
					if a, ok := agents[role.SwarmAgent]; ok {
						return a, nil
					}
					return nil, fmt.Errorf("step %q: SwarmAgent %q not found in loaded YAML", roleName, role.SwarmAgent)
				}
				// Inline role: look for auto-name first, then synthesize.
				autoName := t.Name + "-" + roleName
				if a, ok := agents[autoName]; ok {
					return a, nil
				}
				// Synthesize an ephemeral SwarmAgent from the inline role definition.
				spec := swarmv1alpha1.SwarmAgentSpec{
					Model:    role.Model,
					Prompt:   role.Prompt,
					Settings: role.Settings,
				}
				if role.Tools != nil {
					spec.Tools = role.Tools
				}
				if role.Limits != nil {
					spec.Guardrails = &swarmv1alpha1.AgentGuardrails{Limits: role.Limits}
				}
				return &swarmv1alpha1.SwarmAgent{Spec: spec}, nil
			}
		}
		return nil, fmt.Errorf("step %q: role not found in spec.roles", roleName)
	}

	flow.InitializeRunSteps(run)

	for !flow.IsTerminalRunPhase(run.Status.Phase) {
		statusByName := flow.BuildRunStatusByName(run)
		templateData := flow.BuildRunTemplateData(run, statusByName)
		flow.EvaluateRunLoops(run, statusByName, templateData)

		ready := e.collectReadyRunSteps(run, statusByName, templateData)

		if len(ready) == 0 {
			flow.UpdateRunPipelinePhase(run, templateData)
			break
		}

		type stepResult struct {
			name      string
			output    string
			tokens    queue.TokenUsage
			elapsed   time.Duration
			err       error
			artifacts map[string]string
		}
		results := make(chan stepResult, len(ready))

		for _, step := range ready {
			st := statusByName[step.Role]
			now := metav1.Now()
			st.Phase = swarmv1alpha1.PipelineStepPhaseRunning
			st.StartTime = &now
			onEvent(StepEvent{Step: step.Role, Phase: swarmv1alpha1.PipelineStepPhaseRunning})

			go func(s swarmv1alpha1.SwarmTeamPipelineStep) {
				start := time.Now()
				prompt, _ := flow.ResolveTeamPrompt(s, templateData)
				agent, err := resolveAgent(s.Role)
				var output string
				var tokens queue.TokenUsage
				var execErr error
				if err != nil {
					execErr = err
				} else {
					output, tokens, execErr = e.runStepWithAgent(ctx, s.Role, agent, prompt)
				}
				var stepArtifacts map[string]string
				if execErr == nil && e.ArtifactStore != nil && len(s.OutputArtifacts) > 0 {
					stepArtifacts = e.collectStepArtifacts(ctx, run.Name, s, statusByName)
				}
				results <- stepResult{
					name:      s.Role,
					output:    output,
					tokens:    tokens,
					elapsed:   time.Since(start),
					err:       execErr,
					artifacts: stepArtifacts,
				}
			}(step)
		}

		for range ready {
			res := <-results
			st := statusByName[res.name]

			if res.err != nil {
				now := metav1.Now()
				st.Phase = swarmv1alpha1.PipelineStepPhaseFailed
				st.CompletionTime = &now
				st.Message = res.err.Error()
				onEvent(StepEvent{
					Step:    res.name,
					Phase:   swarmv1alpha1.PipelineStepPhaseFailed,
					Elapsed: res.elapsed,
					Err:     res.err,
				})
				continue
			}

			// Locate the validate config for this step (if any).
			var validate *swarmv1alpha1.StepValidation
			for i := range run.Spec.Pipeline {
				if run.Spec.Pipeline[i].Role == res.name {
					validate = run.Spec.Pipeline[i].Validate
					break
				}
			}

			// Run contains + schema + semantic validation (synchronous).
			if validate != nil {
				passed, reason := flow.ValidateStepOutput(res.output, validate)
				if !passed {
					if evt := e.handleValidationFailure(st, validate, reason, res.elapsed); evt != nil {
						onEvent(*evt)
						continue
					}
					// handleValidationFailure returning nil means it already set Failed.
					// Fall through so we emit the Failed event below.
				} else if validate.Semantic != "" {
					semModel := e.resolveSemanticModel(validate, res.name, resolveAgent)
					if !e.runSemanticValidation(ctx, st, validate, res.output, res.elapsed, semModel, res.name, onEvent) {
						continue
					}
				}
			}

			// Check if validation failure left the step as Failed (terminal).
			if st.Phase == swarmv1alpha1.PipelineStepPhaseFailed {
				onEvent(StepEvent{
					Step:    res.name,
					Phase:   swarmv1alpha1.PipelineStepPhaseFailed,
					Elapsed: res.elapsed,
					Err:     fmt.Errorf("%s", st.Message),
				})
				continue
			}

			// Validation passed (or no validate block configured).
			now := metav1.Now()
			st.Phase = swarmv1alpha1.PipelineStepPhaseSucceeded
			st.CompletionTime = &now
			st.Output = res.output
			st.TokenUsage = &swarmv1alpha1.TokenUsage{
				InputTokens:  res.tokens.InputTokens,
				OutputTokens: res.tokens.OutputTokens,
				TotalTokens:  res.tokens.InputTokens + res.tokens.OutputTokens,
			}
			if len(res.artifacts) > 0 {
				st.Artifacts = res.artifacts
			}
			stepCost := e.calcStepCost(res.name, res.tokens, resolveAgent)
			if stepCost > 0 {
				st.CostUSD = fmt.Sprintf("%.6f", stepCost)
				prev := 0.0
				if run.Status.TotalCostUSD != "" {
					prev, _ = strconv.ParseFloat(run.Status.TotalCostUSD, 64)
				}
				run.Status.TotalCostUSD = fmt.Sprintf("%.6f", prev+stepCost)
			}
			onEvent(StepEvent{
				Step:      res.name,
				Phase:     swarmv1alpha1.PipelineStepPhaseSucceeded,
				Output:    res.output,
				Tokens:    res.tokens,
				CostUSD:   stepCost,
				Elapsed:   res.elapsed,
				Validated: validate != nil,
			})
		}

		flow.ParseRunOutputJSON(run, statusByName)
		templateData = flow.BuildRunTemplateData(run, statusByName)
		flow.UpdateRunPipelinePhase(run, templateData)
	}

	return run, nil
}

// collectReadyRunSteps returns all Pending SwarmRun pipeline steps whose deps are satisfied.
func (e *Executor) collectReadyRunSteps(
	run *swarmv1alpha1.SwarmRun,
	statusByName map[string]*swarmv1alpha1.PipelineStepStatus,
	templateData map[string]any,
) []swarmv1alpha1.SwarmTeamPipelineStep {
	var ready []swarmv1alpha1.SwarmTeamPipelineStep
	for _, step := range run.Spec.Pipeline {
		st := statusByName[step.Role]
		if st == nil || st.Phase != swarmv1alpha1.PipelineStepPhasePending {
			continue
		}
		if !flow.DepsSucceeded(step.DependsOn, statusByName) {
			continue
		}
		if step.If != "" {
			result, err := flow.ResolveTemplate(step.If, templateData)
			if err != nil || !flow.IsTruthy(result) {
				now := metav1.Now()
				st.Phase = swarmv1alpha1.PipelineStepPhaseSkipped
				st.CompletionTime = &now
				st.Message = "skipped: if condition evaluated to false"
				continue
			}
		}
		ready = append(ready, step)
	}
	return ready
}

// runStepWithAgent executes a single step given a resolved SwarmAgent spec.
func (e *Executor) runStepWithAgent(
	ctx context.Context,
	stepName string,
	agent *swarmv1alpha1.SwarmAgent,
	prompt string,
) (string, queue.TokenUsage, error) {
	cfg := agentConfig(agent)
	provider, err := e.Provider(agent.Spec.Model)
	if err != nil {
		return "", queue.TokenUsage{}, fmt.Errorf("step %q: %w", stepName, err)
	}
	mcpMgr := e.buildMCPManager(cfg.MCPServers)
	r := runner.New(cfg, mcpMgr, provider, nil /* no task queue */, nil /* no stream channel */, nil /* no delegate queues */)

	task := queue.Task{ID: stepName, Prompt: prompt}

	stepCtx := ctx
	if cfg.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	return r.RunTask(stepCtx, task)
}

// agentConfig builds a runtime config.Config from an SwarmAgent spec.
//
// Limitations in local mode (no k8s client):
//   - SystemPromptRef cannot be resolved; a warning is embedded when SystemPrompt is empty.
//   - SettingsRefs cannot be resolved; a warning comment is appended to the system prompt.
//
// MCP guidance (MCPServerSpec.guidance) is applied locally because it is available inline
// in the YAML — it is appended as a "## MCP Tool Guidance" section.
func agentConfig(agent *swarmv1alpha1.SwarmAgent) *config.Config {
	var systemPrompt string
	if agent.Spec.Prompt != nil {
		systemPrompt = agent.Spec.Prompt.Inline
		if systemPrompt == "" && agent.Spec.Prompt.From != nil {
			systemPrompt = "[warning: prompt.from cannot be resolved locally - set prompt.inline for swarm run]"
		}
	}

	// settings require a k8s client and cannot be resolved in local mode.
	if len(agent.Spec.Settings) > 0 {
		names := make([]string, 0, len(agent.Spec.Settings))
		for _, r := range agent.Spec.Settings {
			names = append(names, r.Name)
		}
		systemPrompt += "\n\n[warning: settings [" + strings.Join(names, ", ") + "] were skipped - not available in local mode]"
	}

	// Apply MCP instructions inline - available from the YAML without a k8s lookup.
	if agent.Spec.Tools != nil {
		var mcpInstructions []string
		for _, s := range agent.Spec.Tools.MCP {
			if s.Instructions != "" {
				mcpInstructions = append(mcpInstructions, "### "+s.Name+"\n"+s.Instructions)
			}
		}
		if len(mcpInstructions) > 0 {
			systemPrompt += "\n\n## MCP Tool Instructions\n\n" + strings.Join(mcpInstructions, "\n\n")
		}
	}

	cfg := &config.Config{
		Model:            agent.Spec.Model,
		SystemPrompt:     systemPrompt,
		MaxTokensPerCall: 8000,
		TimeoutSeconds:   120,
	}

	if agent.Spec.Guardrails != nil && agent.Spec.Guardrails.Limits != nil {
		limits := agent.Spec.Guardrails.Limits
		if limits.TokensPerCall > 0 {
			cfg.MaxTokensPerCall = limits.TokensPerCall
		}
		if limits.TimeoutSeconds > 0 {
			cfg.TimeoutSeconds = limits.TimeoutSeconds
		}
	}

	if agent.Spec.Tools != nil {
		for _, s := range agent.Spec.Tools.MCP {
			cfg.MCPServers = append(cfg.MCPServers, config.MCPServerConfig{
				Name: s.Name,
				URL:  s.URL,
			})
		}
	}

	return cfg
}

// resolveSemanticModel returns the model name to use for semantic validation.
// Falls back to the step's SwarmAgent model when validate.SemanticModel is empty.
func (e *Executor) resolveSemanticModel(
	v *swarmv1alpha1.StepValidation,
	roleName string,
	resolveAgent func(string) (*swarmv1alpha1.SwarmAgent, error),
) string {
	if v.SemanticModel != "" {
		return v.SemanticModel
	}
	agent, err := resolveAgent(roleName)
	if err != nil {
		return ""
	}
	return agent.Spec.Model
}

// runSemanticValidation executes the semantic validation check for a step.
// Returns true if the step should continue to Succeeded, false if it was retried or failed.
func (e *Executor) runSemanticValidation(
	ctx context.Context,
	st *swarmv1alpha1.PipelineStepStatus,
	v *swarmv1alpha1.StepValidation,
	output string,
	elapsed time.Duration,
	semModel, roleName string,
	onEvent func(StepEvent),
) bool {
	if e.SemanticValidateFn == nil {
		fmt.Fprintf(os.Stderr, "warning: step %q has semantic validation but SemanticValidateFn is not set; skipping\n", roleName)
		return true
	}
	if semModel == "" {
		return true
	}
	prompt := flow.SemanticValidatorPrompt(v.Semantic, output)
	response, semErr := e.SemanticValidateFn(ctx, semModel, prompt)
	var semPassed bool
	var semReason string
	if semErr != nil {
		semPassed = false
		semReason = fmt.Sprintf("semantic validator error: %v", semErr)
	} else {
		semPassed, semReason = flow.ParseSemanticResult(response)
	}
	if !semPassed {
		if evt := e.handleValidationFailure(st, v, semReason, elapsed); evt != nil {
			onEvent(*evt)
			return false
		}
	}
	return true
}

// handleValidationFailure processes a validation failure for a step in RunTeam.
// It increments ValidationAttempts, updates ValidationMessage, and decides
// whether to retry (reset to Pending) or fail terminally.
//
// Returns a non-nil *StepEvent to emit when the step should be re-queued (Pending)
// or left in Failed state for the caller to handle.
// Returns nil when the step is left in Failed state (caller must emit the Failed event).
func (e *Executor) handleValidationFailure(
	st *swarmv1alpha1.PipelineStepStatus,
	v *swarmv1alpha1.StepValidation,
	reason string,
	elapsed time.Duration,
) *StepEvent {
	st.ValidationAttempts++
	st.ValidationMessage = reason

	if v.OnFailure == "retry" && st.ValidationAttempts < v.MaxRetries {
		// Reset step to Pending so the outer loop re-executes it.
		st.Phase = swarmv1alpha1.PipelineStepPhasePending
		st.Output = ""
		st.OutputJSON = ""
		st.TaskID = ""
		st.CompletionTime = nil
		return &StepEvent{
			Step:       st.Name,
			Phase:      swarmv1alpha1.PipelineStepPhasePending,
			Elapsed:    elapsed,
			Err:        fmt.Errorf("%s", reason),
			RetryCount: st.ValidationAttempts,
		}
	}

	// Terminal failure.
	now := metav1.Now()
	st.Phase = swarmv1alpha1.PipelineStepPhaseFailed
	st.CompletionTime = &now
	st.Message = fmt.Sprintf("validation failed: %s", reason)
	return nil
}

// collectStepArtifacts scans the step's artifact directory for declared artifacts,
// uploads each to the ArtifactStore, and returns a name->URL map.
// Files that don't exist are silently skipped (agent may not produce every artifact).
func (e *Executor) collectStepArtifacts(
	ctx context.Context,
	runName string,
	step swarmv1alpha1.SwarmTeamPipelineStep,
	_ map[string]*swarmv1alpha1.PipelineStepStatus,
) map[string]string {
	if e.ArtifactStore == nil || len(step.OutputArtifacts) == 0 {
		return nil
	}
	base := e.ArtifactBaseDir
	if base == "" {
		base = os.Getenv("AGENT_ARTIFACT_DIR")
	}
	if base == "" {
		return nil
	}
	stepDir := filepath.Join(base, step.Role)
	result := make(map[string]string, len(step.OutputArtifacts))
	for _, spec := range step.OutputArtifacts {
		path := filepath.Join(stepDir, spec.Name)
		data, err := os.ReadFile(path) //nolint:gosec
		if err != nil {
			if !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "warning: artifact %q for step %q: %v\n", spec.Name, step.Role, err)
			}
			continue
		}
		url, err := e.ArtifactStore.Put(ctx, runName, step.Role, spec.Name, data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: storing artifact %q for step %q: %v\n", spec.Name, step.Role, err)
			continue
		}
		result[spec.Name] = url
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// buildMCPManager attempts to connect to each MCP server individually,
// silently skipping any that are unreachable. When e.NoMCP is true,
// returns an empty manager without attempting any connections.
func (e *Executor) buildMCPManager(servers []config.MCPServerConfig) *mcp.Manager {
	if e.NoMCP || len(servers) == 0 {
		m, _ := mcp.NewManager(nil)
		return m
	}
	var reachable []config.MCPServerConfig
	for _, s := range servers {
		if _, err := mcp.NewManager([]config.MCPServerConfig{s}); err == nil {
			reachable = append(reachable, s)
		}
	}
	m, _ := mcp.NewManager(reachable)
	return m
}

// calcStepCost returns the estimated USD cost for a completed step using the executor's
// CostProvider. Returns 0 when no CostProvider is configured or the agent cannot be resolved.
func (e *Executor) calcStepCost(
	name string,
	tokens queue.TokenUsage,
	resolveAgent func(string) (*swarmv1alpha1.SwarmAgent, error),
) float64 {
	if e.CostProvider == nil {
		return 0
	}
	agent, err := resolveAgent(name)
	if err != nil {
		return 0
	}
	return e.CostProvider.Cost(agent.Spec.Model, tokens.InputTokens, tokens.OutputTokens)
}

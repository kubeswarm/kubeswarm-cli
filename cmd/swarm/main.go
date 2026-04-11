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

// swarm is a local development CLI for kubeswarm.
// It lets developers run, test, and debug SwarmTeam pipelines locally
// without a Kubernetes cluster, reusing the same YAML files that kubectl apply -f accepts.
//
// Usage:
//
//	swarm run quickstart.yaml
//	swarm run quickstart.yaml --mock
//	swarm run quickstart.yaml --dry-run
//	swarm run quickstart.yaml --watch
//	swarm run quickstart.yaml --output json
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeswarm/kubeswarm-cli/internal/swarm"
	"github.com/kubeswarm/kubeswarm-cli/internal/swarm/local"
	swarmv1alpha1 "github.com/kubeswarm/kubeswarm/api/v1alpha1"
	"github.com/kubeswarm/kubeswarm/pkg/agent/providers"
	_ "github.com/kubeswarm/kubeswarm/pkg/agent/providers/mock"
	"github.com/kubeswarm/kubeswarm/pkg/audit"
	"github.com/kubeswarm/kubeswarm/pkg/costs"
	"github.com/kubeswarm/kubeswarm/pkg/flow"
	"github.com/kubeswarm/kubeswarm/pkg/observability"
	"github.com/kubeswarm/kubeswarm/pkg/validation"
	_ "github.com/kubeswarm/kubeswarm/runtime/providers/anthropic"
	_ "github.com/kubeswarm/kubeswarm/runtime/providers/gemini"
	_ "github.com/kubeswarm/kubeswarm/runtime/providers/openai"
)

// version is set at build time via -ldflags "-X main.version=<tag>".
// Falls back to the module version embedded by go install, or "dev" for local builds.
var version = "dev"

func init() {
	if version == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
	}
}

const outputFormatJSON = "json"

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:     "swarm",
		Short:   "Local development CLI for kubeswarm",
		Version: version,
		Long: `swarm lets you run, test, and debug SwarmTeam pipelines locally
without a Kubernetes cluster. The same YAML files work unchanged
with kubectl apply -f when you are ready to deploy.`,
	}
	root.AddCommand(runCmd())
	root.AddCommand(validateCmd())
	root.AddCommand(initCmd())
	root.AddCommand(traceCmd())
	root.AddCommand(retryCmd())
	root.AddCommand(triggerCmd())
	root.AddCommand(submitCmd())
	root.AddCommand(runsCmd())
	root.AddCommand(operatorCmd())
	root.AddCommand(deployCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(registryCmd())
	root.AddCommand(mcpCmd())
	root.AddCommand(agentCmd())
	root.AddCommand(auditCmd())
	return root
}

// runFlags holds the parsed flags for the run subcommand.
type runFlags struct {
	provider string
	flow     string
	dryRun   bool
	watch    bool
	noMCP    bool
	output   string
	input    string
	trace    bool
}

func runCmd() *cobra.Command {
	var f runFlags

	cmd := &cobra.Command{
		Use:   "run <file>",
		Short: "Execute an SwarmTeam pipeline locally",
		Long: `Run an SwarmTeam pipeline defined in a multi-document YAML file.
The file may contain SwarmAgent, SwarmTeam, and other resource definitions —
only SwarmAgent and SwarmTeam (pipeline mode) documents are used; everything else is ignored.

Examples:
  swarm run quickstart.yaml
  swarm run quickstart.yaml --provider mock
  swarm run quickstart.yaml --provider openai --watch
  swarm run quickstart.yaml --watch --output json
  swarm run pipeline.yaml --input '{"topic":"Kubernetes operators"}'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFlow(cmd.Context(), args[0], f)
		},
	}

	cmd.Flags().StringVar(&f.provider, "provider", "auto", "LLM provider: auto, anthropic, openai, or mock")
	cmd.Flags().StringVar(&f.flow, "flow", "", "Team/flow name to run when the file contains multiple pipeline resources")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "Validate YAML and print a summary without executing")
	cmd.Flags().BoolVar(&f.watch, "watch", false, "Stream step-by-step output as the pipeline executes")
	cmd.Flags().BoolVar(&f.noMCP, "no-mcp", false, "Skip MCP tool server connections")
	cmd.Flags().StringVar(&f.output, "output", "text", "Output format: text or json")
	cmd.Flags().StringVar(&f.input, "input", "", `Pipeline input as a JSON object, e.g. '{"topic":"Kubernetes"}'`)
	cmd.Flags().BoolVar(&f.trace, "trace", false,
		"Print a span tree after the pipeline completes (local trace, no backend required)")

	return cmd
}

func runFlow(ctx context.Context, path string, flags runFlags) error {
	teams, agents, err := swarm.LoadFile(path)
	if err != nil {
		return err
	}

	if len(teams) == 0 {
		return fmt.Errorf("no SwarmTeam pipeline found in %s", path)
	}
	return runTeam(ctx, teams, agents, flags)
}

func runTeam(ctx context.Context, teams []*swarmv1alpha1.SwarmTeam, agents map[string]*swarmv1alpha1.SwarmAgent, flags runFlags) error { //nolint:dupl,lll
	t, err := selectTeam(teams, flags.flow)
	if err != nil {
		return err
	}

	if flags.input != "" {
		var inputMap map[string]string
		if err := json.Unmarshal([]byte(flags.input), &inputMap); err != nil {
			return fmt.Errorf("--input must be a JSON object of string values: %w", err)
		}
		t.Spec.Input = inputMap
	}

	if flags.dryRun {
		return dryRunTeam(t, agents)
	}

	var col *observability.SpanCollector
	if flags.trace {
		var shutdown func()
		var initErr error
		col, shutdown, initErr = observability.InitCollector(ctx, "kubeswarm-run")
		if initErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to initialise trace collector: %v\n", initErr)
			col = nil
		} else {
			defer shutdown()
		}
	}

	executor := &local.Executor{
		Provider:           buildProviderFunc(flags.provider),
		NoMCP:              flags.noMCP,
		SemanticValidateFn: validation.BuildSemanticValidateFn(),
		CostProvider:       &costs.StaticCostProvider{},
	}

	start := time.Now()
	onEvent := buildEventHandler(flags.watch, flags.output)

	run, err := executor.RunTeam(ctx, t, agents, onEvent)
	if err != nil {
		return err
	}

	if col != nil {
		observability.PrintTraceTree(os.Stdout, col.Spans())
	}

	return printRunResult(run, time.Since(start), flags.output)
}

// selectTeam picks the team to run.
func selectTeam(teams []*swarmv1alpha1.SwarmTeam, name string) (*swarmv1alpha1.SwarmTeam, error) {
	if name != "" {
		for _, t := range teams {
			if t.Name == name {
				return t, nil
			}
		}
		names := make([]string, len(teams))
		for i, t := range teams {
			names[i] = t.Name
		}
		return nil, fmt.Errorf("team %q not found; available: %v", name, names)
	}
	if len(teams) > 1 {
		names := make([]string, len(teams))
		for i, t := range teams {
			names[i] = t.Name
		}
		fmt.Fprintf(os.Stderr, "warning: %d teams found %v — running %q (use --flow <name> to pick)\n",
			len(teams), names, teams[0].Name)
	}
	return teams[0], nil
}

// validateCmd returns the `swarm validate` subcommand.
func validateCmd() *cobra.Command {
	var flowName string
	cmd := &cobra.Command{
		Use:   "validate <file>",
		Short: "Validate an SwarmTeam pipeline YAML without executing it",
		Long: `Parse the YAML, check DAG integrity, and verify that every step
references a known SwarmAgent. Exits 0 on success, non-zero on error.

Examples:
  swarm validate quickstart.yaml
  swarm validate pipeline.yaml --flow my-team`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			teams, agents, err := swarm.LoadFile(args[0])
			if err != nil {
				return err
			}
			if len(teams) == 0 {
				return fmt.Errorf("no SwarmTeam resources found in %s", args[0])
			}
			t, err := selectTeam(teams, flowName)
			if err != nil {
				return err
			}
			return dryRunTeam(t, agents)
		},
	}
	cmd.Flags().StringVar(&flowName, "flow", "",
		"Team/flow name to validate when the file contains multiple pipeline resources")
	return cmd
}

// dryRunTeam validates an SwarmTeam pipeline and prints a summary without executing.
func dryRunTeam(t *swarmv1alpha1.SwarmTeam, agents map[string]*swarmv1alpha1.SwarmAgent) error {
	if len(t.Spec.Pipeline) == 0 {
		fmt.Printf("Team:  %s (dynamic mode — no pipeline to validate locally)\n", t.Name)
		fmt.Println("✓ YAML is valid — deploy to a cluster to run dynamic mode")
		return nil
	}
	if err := flow.ValidateTeamDAG(t); err != nil {
		return fmt.Errorf("invalid DAG: %w", err)
	}

	var missingAgents []string
	fmt.Printf("Team:   %s\n", t.Name)
	fmt.Printf("Steps:  %d\n", len(t.Spec.Pipeline))
	for _, step := range t.Spec.Pipeline {
		// Resolve agent name: external ref or inline.
		agentModel := "<not found>"
		for _, role := range t.Spec.Roles {
			if role.Name == step.Role {
				if role.SwarmAgent != "" {
					if a, ok := agents[role.SwarmAgent]; ok {
						agentModel = a.Spec.Model
					} else {
						missingAgents = append(missingAgents,
							fmt.Sprintf("step %q references unknown SwarmAgent %q", step.Role, role.SwarmAgent))
					}
				} else if role.Model != "" {
					agentModel = role.Model + " (inline)"
				}
				break
			}
		}
		deps := "-"
		if len(step.DependsOn) > 0 {
			deps = fmt.Sprintf("%v", step.DependsOn)
		}
		fmt.Printf("  %-20s  model=%-25s  deps=%s\n", step.Role, agentModel, deps)
	}
	if len(missingAgents) > 0 {
		for _, msg := range missingAgents {
			fmt.Fprintf(os.Stderr, "error: %s\n", msg)
		}
		return fmt.Errorf("%d missing agent(s) — add the SwarmAgent definitions to your YAML", len(missingAgents))
	}
	fmt.Println("✓ YAML is valid — use `swarm run` to execute")
	return nil
}

// buildProviderFunc returns a per-model provider lookup function.
//
// When flag is "auto" (the default), the provider is detected from the model
// name at step execution time — claude-* uses anthropic, gpt-*/o* uses openai.
// Any other value pins every step to that specific provider.
func buildProviderFunc(flag string) func(model string) (providers.LLMProvider, error) {
	return func(model string) (providers.LLMProvider, error) {
		name := flag
		if name == "auto" {
			name = providers.Detect(model)
		}
		p, err := providers.New(name)
		if err != nil {
			return nil, fmt.Errorf("unknown provider %q — supported: anthropic, openai, mock", name)
		}
		return p, nil
	}
}

// buildEventHandler returns a function that prints progress for --watch mode.
// In PR 4 this will be replaced with rich terminal output; for now it emits
// simple one-line status updates.
func buildEventHandler(watch bool, outputFmt string) func(local.StepEvent) {
	if !watch || outputFmt == outputFormatJSON {
		return func(local.StepEvent) {} // silent until pipeline completes
	}
	return func(evt local.StepEvent) {
		switch evt.Phase {
		case swarmv1alpha1.PipelineStepPhaseRunning:
			fmt.Printf("  %-20s [running]\n", evt.Step)
		case swarmv1alpha1.PipelineStepPhaseSucceeded:
			tokens := evt.Tokens.InputTokens + evt.Tokens.OutputTokens
			costStr := ""
			if evt.CostUSD > 0 {
				costStr = fmt.Sprintf("  $%.4f", evt.CostUSD)
			}
			if evt.Validated {
				fmt.Printf("  %-20s [done]    %d tokens%s  %.1fs  ✓ validated\n", evt.Step, tokens, costStr, evt.Elapsed.Seconds())
			} else {
				fmt.Printf("  %-20s [done]    %d tokens%s  %.1fs\n", evt.Step, tokens, costStr, evt.Elapsed.Seconds())
			}
			if evt.Output != "" {
				preview := evt.Output
				if len(preview) > 120 {
					preview = preview[:120] + "..."
				}
				fmt.Printf("    └─ %s\n", preview)
			}
		case swarmv1alpha1.PipelineStepPhaseSkipped:
			fmt.Printf("  %-20s [skipped]\n", evt.Step)
		case swarmv1alpha1.PipelineStepPhaseFailed:
			fmt.Printf("  %-20s [failed]  %v\n", evt.Step, evt.Err)
		case swarmv1alpha1.PipelineStepPhasePending:
			if evt.RetryCount > 0 {
				fmt.Printf("  %-20s [retry %d] validation failed: %v\n", evt.Step, evt.RetryCount, evt.Err)
			}
		}
	}
}

// printRunResult prints the final SwarmRun pipeline result.
func printRunResult(run *swarmv1alpha1.SwarmRun, elapsed time.Duration, outputFmt string) error {
	if outputFmt == outputFormatJSON {
		return printRunJSON(run, elapsed)
	}
	return printRunText(run, elapsed)
}

func printRunText(run *swarmv1alpha1.SwarmRun, elapsed time.Duration) error {
	total := int64(0)
	if run.Status.TotalTokenUsage != nil {
		total = run.Status.TotalTokenUsage.TotalTokens
	}
	phase := string(run.Status.Phase)
	costStr := ""
	if run.Status.TotalCostUSD != "" && run.Status.TotalCostUSD != "0" {
		costStr = fmt.Sprintf("  $%s", run.Status.TotalCostUSD)
	}
	fmt.Printf("\nPipeline %s in %.1fs — total: %d tokens%s\n", phase, elapsed.Seconds(), total, costStr)
	if run.Status.Output != "" {
		fmt.Printf("\nOutput:\n%s\n", run.Status.Output)
	}
	if run.Status.Phase == swarmv1alpha1.SwarmRunPhaseFailed {
		return fmt.Errorf("pipeline failed")
	}
	return nil
}

func printRunJSON(run *swarmv1alpha1.SwarmRun, elapsed time.Duration) error {
	type stepOut struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Tokens  int64  `json:"tokens"`
		CostUSD string `json:"cost_usd,omitempty"`
		Output  string `json:"output,omitempty"`
	}
	type result struct {
		Status       string    `json:"status"`
		DurationMS   int64     `json:"duration_ms"`
		TotalTokens  int64     `json:"total_tokens"`
		TotalCostUSD string    `json:"total_cost_usd,omitempty"`
		Output       string    `json:"output,omitempty"`
		Steps        []stepOut `json:"steps"`
	}
	r := result{
		Status:       string(run.Status.Phase),
		DurationMS:   elapsed.Milliseconds(),
		Output:       run.Status.Output,
		TotalCostUSD: run.Status.TotalCostUSD,
	}
	if run.Status.TotalTokenUsage != nil {
		r.TotalTokens = run.Status.TotalTokenUsage.TotalTokens
	}
	for _, st := range run.Status.Steps {
		tokens := int64(0)
		if st.TokenUsage != nil {
			tokens = st.TokenUsage.TotalTokens
		}
		r.Steps = append(r.Steps, stepOut{
			Name:    st.Name,
			Status:  string(st.Phase),
			Tokens:  tokens,
			CostUSD: st.CostUSD,
			Output:  st.Output,
		})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return err
	}
	if run.Status.Phase == swarmv1alpha1.SwarmRunPhaseFailed {
		return fmt.Errorf("pipeline failed")
	}
	return nil
}

// initCmd returns the `swarm init` subcommand.
func initCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init <project>",
		Short: "Scaffold a new kubeswarm project",
		Long: `Create a new directory with a ready-to-run SwarmTeam project.

The generated project contains:
  quickstart.yaml   — SwarmAgent + SwarmTeam definition
  .env.example      — required environment variables
  docker-compose.yml — local Redis for task queue

Examples:
  swarm init my-agent
  cd my-agent && swarm run quickstart.yaml --provider mock`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return initProject(args[0])
		},
	}
}

func initProject(name string) error {
	if _, err := os.Stat(name); err == nil {
		return fmt.Errorf("directory %q already exists", name)
	}
	if err := os.MkdirAll(name, 0750); err != nil {
		return err
	}

	files := map[string]string{
		"quickstart.yaml":    initQuickstartYAML(name),
		".env.example":       initEnvExample,
		"docker-compose.yml": initDockerCompose,
	}
	for filename, content := range files {
		path := fmt.Sprintf("%s/%s", name, filename)
		//nolint:gosec // scaffolded project files (docker-compose.yml etc.) are intended to be world-readable
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			return err
		}
	}

	fmt.Printf("Created project %q\n\n", name)
	fmt.Println("Next steps:")
	fmt.Printf("  cd %s\n", name)
	fmt.Println("  swarm run quickstart.yaml --provider mock --watch")
	fmt.Println("  swarm run quickstart.yaml --provider anthropic --watch")
	return nil
}

func initQuickstartYAML(name string) string {
	return fmt.Sprintf(`apiVersion: kubeswarm.io/v1alpha1
kind: SwarmTeam
metadata:
  name: %s-team
spec:
  input:
    question: "What is a Kubernetes operator?"
  roles:
    - name: answerer
      model: claude-sonnet-4-20250514
      systemPrompt: |
        You are a helpful assistant. Answer questions clearly and concisely.
      limits:
        maxTokensPerCall: 4000
        timeoutSeconds: 60
  pipeline:
    - role: answerer
      inputs:
        prompt: "{{ .input.question }}"
  output: "{{ .steps.answerer.output }}"
`, name)
}

const initEnvExample = `# Copy this file to .env and fill in your API keys.

# Required: LLM provider API key.
ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...

# Required for Kubernetes deployments (not needed for swarm run locally).
TASK_QUEUE_URL=redis.kubeswarm-system.svc.cluster.local:6379
`

const initDockerCompose = `# Local Redis for task queue — used when running the operator in a cluster.
# Not required for swarm run locally.
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
`

// traceCmd returns the `swarm trace <task-id>` subcommand.
// It queries a Jaeger-compatible HTTP API for spans tagged with task.id=<task-id>
// and prints the result as a tree.
func traceCmd() *cobra.Command {
	var endpoint string
	var service string

	cmd := &cobra.Command{
		Use:   "trace <task-id>",
		Short: "Fetch and display the trace for a task",
		Long: `Query a Jaeger-compatible trace backend for all spans associated with a task ID
and display them as a tree. Requires a running Jaeger or Tempo instance.

The backend endpoint is read from JAEGER_ENDPOINT or TEMPO_ENDPOINT (in that order),
or set explicitly with --endpoint.

Examples:
  swarm trace abc-123
  swarm trace abc-123 --endpoint http://jaeger.monitoring.svc:16686
  swarm trace abc-123 --endpoint http://tempo.monitoring.svc:3200`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTrace(cmd.Context(), args[0], endpoint, service)
		},
	}

	defaultEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if defaultEndpoint == "" {
		defaultEndpoint = os.Getenv("TEMPO_ENDPOINT")
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", defaultEndpoint,
		"Trace backend base URL (Jaeger: :16686, Tempo: :3200). Falls back to JAEGER_ENDPOINT / TEMPO_ENDPOINT env vars.")
	cmd.Flags().StringVar(&service, "service", "kubeswarm-runtime",
		"OTel service name to search within")
	return cmd
}

func runTrace(ctx context.Context, taskID, endpoint, service string) error {
	if endpoint == "" {
		return fmt.Errorf("no trace backend endpoint — set JAEGER_ENDPOINT, TEMPO_ENDPOINT, or use --endpoint")
	}

	spans, err := observability.FetchTraceByTaskID(ctx, endpoint, service, taskID)
	if err != nil {
		return fmt.Errorf("fetching trace for task %q: %w", taskID, err)
	}
	if len(spans) == 0 {
		fmt.Printf("No spans found for task %q (service=%q)\n", taskID, service)
		fmt.Println("Tip: ensure OTEL_EXPORTER_OTLP_ENDPOINT is set in the agent pod and traces have been ingested.")
		return nil
	}

	observability.PrintRemoteTraceTree(os.Stdout, spans)
	return nil
}

// ─── swarm retry ───────────────────────────────────────────────────────────────

func retryCmd() *cobra.Command {
	var namespace string
	var kubeContext string
	var stepName string

	cmd := &cobra.Command{
		Use:   "retry <team>",
		Short: "Retry failed steps in a cluster SwarmTeam pipeline",
		Long: `Reset failed steps back to Pending so the operator re-submits them.
Steps that already succeeded are left untouched.

Use --step to retry a single step instead of all failed steps.

Examples:
  swarm retry my-team -n my-namespace
  swarm retry my-team -n my-namespace --step summarize
  swarm retry my-team -n my-namespace --context prod-cluster`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRetry(cmd.Context(), args[0], namespace, kubeContext, stepName)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmTeam (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&stepName, "step", "", "Retry only this step (default: all failed steps)")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runRetry(ctx context.Context, name, namespace, kubeContext, stepName string) error {
	run, err := getLatestSwarmRun(ctx, name, namespace, kubeContext)
	if err != nil {
		return err
	}
	if run == nil {
		return fmt.Errorf("no SwarmRun found for team %q", name)
	}

	if run.Status.Phase != swarmv1alpha1.SwarmRunPhaseFailed {
		return fmt.Errorf("latest run %q is %q — only Failed runs can be retried", run.Name, run.Status.Phase)
	}

	resetCount := resetFailedRunSteps(run, stepName)
	if resetCount == 0 {
		if stepName != "" {
			return fmt.Errorf("step %q is not in Failed state", stepName)
		}
		return fmt.Errorf("no failed steps found in run %q", run.Name)
	}

	run.Status.Phase = swarmv1alpha1.SwarmRunPhaseRunning
	run.Status.CompletionTime = nil

	if err := patchSwarmRunStatus(ctx, run.Name, namespace, kubeContext, run); err != nil {
		return err
	}

	fmt.Printf("Retrying %d step(s) in run %q — operator will re-submit them shortly.\n",
		resetCount, run.Name)
	return nil
}

// ─── swarm trigger ─────────────────────────────────────────────────────────────

func triggerCmd() *cobra.Command {
	var namespace string
	var kubeContext string
	var inputJSON string
	var agentName string

	cmd := &cobra.Command{
		Use:   "trigger <team|prompt>",
		Short: "Trigger a cluster SwarmTeam pipeline or a standalone SwarmAgent run",
		Long: `Create an SwarmRun on a live cluster.

Team mode (default): creates an SwarmRun with a snapshot of the current team spec.
  Works on teams in any state. Optionally override spec.input values.

Agent mode (--agent): creates an SwarmRun with spec.agent + spec.prompt.
  The SwarmRun controller picks it up and submits the prompt to the agent's queue.

Examples:
  swarm trigger my-team -n my-namespace
  swarm trigger my-team -n my-namespace --input '{"topic":"Kubernetes operators"}'
  swarm trigger "Summarize last week's alerts" --agent monitor-agent -n kubeswarm-teams
  swarm trigger --agent analyst-agent "Which customers have overdue invoices?" -n kubeswarm-teams`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if agentName != "" {
				return runTriggerAgent(cmd.Context(), agentName, args[0], namespace, kubeContext)
			}
			return runTrigger(cmd.Context(), args[0], namespace, kubeContext, inputJSON)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&inputJSON, "input", "",
		`Override team inputs as a JSON object, e.g. '{"topic":"Kubernetes"}'`)
	cmd.Flags().StringVar(&agentName, "agent", "",
		"Create an agent-mode SwarmRun: the positional argument becomes the prompt")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runTrigger(ctx context.Context, name, namespace, kubeContext, inputJSON string) error {
	t, err := getSwarmTeam(ctx, name, namespace, kubeContext)
	if err != nil {
		return err
	}

	// Merge --input overrides on top of the team's default spec.input.
	inputMap := make(map[string]string)
	maps.Copy(inputMap, t.Spec.Input)
	if inputJSON != "" {
		var overrides map[string]string
		if err := json.Unmarshal([]byte(inputJSON), &overrides); err != nil {
			return fmt.Errorf("--input must be a JSON object of string values: %w", err)
		}
		maps.Copy(inputMap, overrides)
	}

	// Create a new SwarmRun with a snapshot of the current team spec.
	runName := fmt.Sprintf("%s-%s", name, time.Now().UTC().Format("20060102150405"))
	trueVal := true
	run := &swarmv1alpha1.SwarmRun{}
	run.APIVersion = "kubeswarm.io/v1alpha1"
	run.Kind = "SwarmRun"
	run.Name = runName
	run.Namespace = namespace
	run.Labels = map[string]string{"kubeswarm.io/team": name}
	run.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         "kubeswarm.io/v1alpha1",
			Kind:               "SwarmTeam",
			Name:               t.Name,
			UID:                t.UID,
			Controller:         &trueVal,
			BlockOwnerDeletion: &trueVal,
		},
	}
	run.Spec.TeamRef = name
	run.Spec.TeamGeneration = t.Generation
	run.Spec.Input = inputMap
	run.Spec.Pipeline = t.Spec.Pipeline
	run.Spec.Roles = t.Spec.Roles
	run.Spec.Routing = t.Spec.Routing
	run.Spec.Output = t.Spec.Output
	run.Spec.TimeoutSeconds = t.Spec.TimeoutSeconds
	run.Spec.MaxTokens = t.Spec.MaxTokens

	if err := createSwarmRun(ctx, run, namespace, kubeContext); err != nil {
		return err
	}

	fmt.Printf("Run %q created for team %q — SwarmRun controller will execute it shortly.\n", runName, name)
	return nil
}

func runTriggerAgent(ctx context.Context, agentName, prompt, namespace, kubeContext string) error {
	// Verify the agent exists so the error message is meaningful.
	agent, err := getSwarmAgent(ctx, agentName, namespace, kubeContext)
	if err != nil {
		return err
	}

	// Snapshot the agent's timeout so the run has the same limit.
	timeoutSeconds := 0
	if agent.Spec.Guardrails != nil && agent.Spec.Guardrails.Limits != nil {
		timeoutSeconds = agent.Spec.Guardrails.Limits.TimeoutSeconds
	}

	runName := fmt.Sprintf("%s-%s", agentName, time.Now().UTC().Format("20060102150405"))
	run := &swarmv1alpha1.SwarmRun{}
	run.APIVersion = "kubeswarm.io/v1alpha1"
	run.Kind = "SwarmRun"
	run.Name = runName
	run.Namespace = namespace
	run.Labels = map[string]string{"kubeswarm.io/agent": agentName}
	run.Spec.Agent = agentName
	run.Spec.Prompt = prompt
	run.Spec.TimeoutSeconds = timeoutSeconds

	if err := createSwarmRun(ctx, run, namespace, kubeContext); err != nil {
		return err
	}

	fmt.Printf("Run %q created for agent %q — SwarmRun controller will execute it shortly.\n", runName, agentName)
	return nil
}

// ─── swarm submit ──────────────────────────────────────────────────────────────

func submitCmd() *cobra.Command {
	var namespace string
	var kubeContext string
	var role string
	var agentName string
	var metaJSON string

	cmd := &cobra.Command{
		Use:   "submit [<team>] <prompt>",
		Short: "Submit a task to a dynamic SwarmTeam's entry role or a standalone SwarmAgent",
		Long: `Submit a task prompt to a dynamic (non-pipeline) SwarmTeam or directly to a
standalone SwarmAgent (e.g. one exposed via the MCP gateway).

Team mode (default): the task is XADDed to the entry role's Redis stream.
  The entry role is read from spec.entry on the SwarmTeam; use --role to override.

Agent mode (--agent): the task is submitted directly to the named SwarmAgent's
  task queue, bypassing any team routing. Use this for standalone agents that
  are not part of an SwarmTeam pipeline.

Examples:
  swarm submit doc-analysis-team "Analyze this contract: ..." -n kubeswarm-teams
  swarm submit doc-analysis-team "Summarize results" -n kubeswarm-teams --role supervisor
  swarm submit --agent analyst-agent "Which customers have shipped orders?" -n kubeswarm-teams
  swarm submit --agent analyst-agent "Show pending orders" -n kubeswarm-teams --meta '{"priority":"high"}'`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if agentName != "" {
				if len(args) != 1 {
					return fmt.Errorf("--agent requires exactly one argument: the prompt")
				}
				return runSubmitAgent(cmd.Context(), agentName, args[0], namespace, kubeContext, metaJSON)
			}
			if len(args) != 2 {
				return fmt.Errorf("submit requires <team> <prompt> (or use --agent <name> <prompt>)")
			}
			return runSubmit(cmd.Context(), args[0], args[1], role, namespace, kubeContext, metaJSON)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&role, "role", "", "Target role name (defaults to spec.entry; team mode only)")
	cmd.Flags().StringVar(&agentName, "agent", "", "Submit directly to a named SwarmAgent instead of a team")
	cmd.Flags().StringVar(&metaJSON, "meta", "", `Extra metadata as a JSON object, e.g. '{"priority":"high"}'`)
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runSubmit(ctx context.Context, teamName, prompt, roleName, namespace, kubeContext, metaJSON string) error {
	// 1. Fetch the SwarmTeam to resolve the entry role.
	t, err := getSwarmTeam(ctx, teamName, namespace, kubeContext)
	if err != nil {
		return err
	}

	if roleName == "" {
		roleName = t.Spec.Entry
	}
	if roleName == "" {
		return fmt.Errorf("team %q has no spec.entry — use --role to specify a target role", teamName)
	}

	// 2. Get the SwarmAgent for this role to find its queue URL annotation.
	agentName := teamName + "-" + roleName
	// Check if the role uses an explicit SwarmAgent reference.
	for _, r := range t.Spec.Roles {
		if r.Name == roleName && r.SwarmAgent != "" {
			agentName = r.SwarmAgent
			break
		}
	}

	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmagent", agentName, "-o",
		`jsonpath={.metadata.annotations.kubeswarm.io/team-queue-url}`)
	queueURL, err := kubectlOutput(ctx, args...)
	if err != nil {
		return fmt.Errorf("fetching SwarmAgent %q: %w", agentName, err)
	}
	if queueURL == "" {
		return fmt.Errorf("SwarmAgent %q has no team-queue-url annotation — is the team reconciled?", agentName)
	}

	// 3. Parse the ?stream= param to get the Redis stream name and host.
	u, err := url.Parse(queueURL)
	if err != nil {
		return fmt.Errorf("parsing queue URL %q: %w", queueURL, err)
	}
	streamName := u.Query().Get("stream")
	if streamName == "" {
		streamName = "agent-tasks"
	}

	// 4. Find the Redis pod and XADD the task.
	redisPod := findRedisPod(ctx, kubeContext)

	xaddArgs := []string{
		"exec", "-n", "agent-infra", redisPod, "--",
		"redis-cli", "XADD", streamName, "*",
		"prompt", prompt,
		"agent", agentName,
		"enqueued_at", time.Now().UTC().Format(time.RFC3339),
	}
	// Append any extra meta key-value pairs.
	if metaJSON != "" {
		var meta map[string]string
		if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
			return fmt.Errorf("--meta must be a JSON object of string values: %w", err)
		}
		for k, v := range meta {
			xaddArgs = append(xaddArgs, k, v)
		}
	}

	out, err := kubectlOutput(ctx, xaddArgs...)
	if err != nil {
		return fmt.Errorf("submitting task to Redis: %w", err)
	}

	fmt.Printf("Task submitted to team %q role %q — task ID: %s\n", teamName, roleName, strings.TrimSpace(out))
	return nil
}

// runSubmitAgent submits a task directly to a named SwarmAgent's task queue,
// without going through an SwarmTeam. Used for standalone agents (e.g. those
// exposed via the MCP gateway) that are not part of a pipeline.
//
// Queue URL resolution order:
//  1. kubeswarm.io/team-queue-url annotation — set by SwarmTeamReconciler on team-role agents.
//  2. TASK_QUEUE_URL env var from the agent's Deployment — injected by the operator
//     for all agent pods (standalone agents included).
func runSubmitAgent(ctx context.Context, agentName, prompt, namespace, kubeContext, metaJSON string) error {
	// 1. Verify the agent exists.
	agent, err := getSwarmAgent(ctx, agentName, namespace, kubeContext)
	if err != nil {
		return err
	}

	// 2. Resolve queue URL.
	queueURL := ""
	if u, ok := agent.Annotations["kubeswarm.io/team-queue-url"]; ok && u != "" {
		queueURL = u
	} else {
		// Standalone agent: read TASK_QUEUE_URL from the Deployment's env vars.
		// The operator injects it at reconcile time; it is not stored in a Secret.
		deployName := agentName + "-agent"
		args := buildKubectlBase(kubeContext, namespace, false)
		args = append(args, "get", "deployment", deployName, "-o",
			`jsonpath={.spec.template.spec.containers[0].env[?(@.name=="TASK_QUEUE_URL")].value}`)
		val, err := kubectlOutput(ctx, args...)
		if err != nil {
			return fmt.Errorf("reading Deployment %q: %w", deployName, err)
		}
		queueURL = strings.TrimSpace(val)
	}
	if queueURL == "" {
		return fmt.Errorf("no TASK_QUEUE_URL found for agent %q — is the operator reconciled?", agentName)
	}

	// 3. Parse the ?stream= param to get the Redis stream name.
	u, err := url.Parse(queueURL)
	if err != nil {
		return fmt.Errorf("parsing queue URL %q: %w", queueURL, err)
	}
	streamName := u.Query().Get("stream")
	if streamName == "" {
		streamName = "agent-tasks"
	}

	// 4. Locate the Redis pod from the queue URL hostname (e.g. kubeswarm-redis.kubeswarm-system.svc.*).
	redisPod, redisNS := findRedisPodFromURL(ctx, u.Hostname(), kubeContext)

	// 5. XADD the task.
	xaddArgs := []string{
		"exec", "-n", redisNS, redisPod, "--",
		"redis-cli", "XADD", streamName, "*",
		"prompt", prompt,
		"enqueued_at", time.Now().UTC().Format(time.RFC3339),
	}
	if metaJSON != "" {
		var meta map[string]string
		if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
			return fmt.Errorf("--meta must be a JSON object of string values: %w", err)
		}
		for k, v := range meta {
			xaddArgs = append(xaddArgs, k, v)
		}
	}

	out, err := kubectlOutput(ctx, xaddArgs...)
	if err != nil {
		return fmt.Errorf("submitting task to Redis: %w", err)
	}

	fmt.Printf("Task submitted to agent %q (stream: %s) — task ID: %s\n",
		agentName, streamName, strings.TrimSpace(out))
	return nil
}

// findRedisPodFromURL locates the Redis pod by parsing the k8s service DNS hostname
// embedded in the queue URL (e.g. "kubeswarm-redis.kubeswarm-system.svc.cluster.local").
// Falls back to findRedisPod (agent-infra namespace) when the hostname is not a
// recognisable in-cluster DNS name.
func findRedisPodFromURL(ctx context.Context, hostname, kubeContext string) (pod, ns string) {
	// k8s service DNS: <service>.<namespace>.svc[.<cluster-domain>]
	parts := strings.Split(hostname, ".")
	if len(parts) >= 3 && parts[2] == "svc" {
		svcName, svcNS := parts[0], parts[1]
		// Resolve via endpoints: the pod backing the service is listed there.
		args := buildKubectlBase(kubeContext, svcNS, false)
		args = append(args, "get", "endpoints", svcName, "-o",
			`jsonpath={.subsets[0].addresses[0].targetRef.name}`)
		if name, err := kubectlOutput(ctx, args...); err == nil && strings.TrimSpace(name) != "" {
			return strings.TrimSpace(name), svcNS
		}
		// Endpoints may not list a pod (e.g. headless StatefulSet with externalName).
		// Fall back to the StatefulSet pod naming convention: <service>-0.
		return svcName + "-0", svcNS
	}
	// Non-cluster hostname: use the legacy agent-infra lookup.
	return findRedisPod(ctx, kubeContext), "agent-infra"
}

// getSwarmAgent fetches an SwarmAgent from the cluster via kubectl.
func getSwarmAgent(ctx context.Context, name, namespace, kubeContext string) (*swarmv1alpha1.SwarmAgent, error) {
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmagent", name, "-o", "json")
	out, err := kubectlOutput(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("getting SwarmAgent %q: %w", name, err)
	}
	var a swarmv1alpha1.SwarmAgent
	if err := json.Unmarshal([]byte(out), &a); err != nil {
		return nil, fmt.Errorf("parsing SwarmAgent %q: %w", name, err)
	}
	return &a, nil
}

// findRedisPod returns the name of the first running Redis pod in agent-infra.
func findRedisPod(ctx context.Context, kubeContext string) string {
	args := buildKubectlBase(kubeContext, "agent-infra", false)
	args = append(args, "get", "pods", "-l", "app.kubernetes.io/name=redis",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod, err := kubectlOutput(ctx, args...)
	if err != nil || strings.TrimSpace(pod) == "" {
		// Fallback: try StatefulSet pod naming convention.
		return "redis-0"
	}
	return strings.TrimSpace(pod)
}

// ─── shared team helpers ──────────────────────────────────────────────────────

// getSwarmTeam fetches an SwarmTeam from the cluster via kubectl.
func getSwarmTeam(ctx context.Context, name, namespace, kubeContext string) (*swarmv1alpha1.SwarmTeam, error) {
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmteam", name, "-o", "json")
	out, err := kubectlOutput(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("getting SwarmTeam %q: %w", name, err)
	}
	var t swarmv1alpha1.SwarmTeam
	if err := json.Unmarshal([]byte(out), &t); err != nil {
		return nil, fmt.Errorf("parsing SwarmTeam %q: %w", name, err)
	}
	return &t, nil
}

// resetFailedRunSteps resets steps in Failed phase back to Pending.
// If stepName is set, only that step is reset. Returns the number of steps reset.
func resetFailedRunSteps(run *swarmv1alpha1.SwarmRun, stepName string) int {
	count := 0
	for i, st := range run.Status.Steps {
		if st.Phase != swarmv1alpha1.PipelineStepPhaseFailed {
			continue
		}
		if stepName != "" && st.Name != stepName {
			continue
		}
		run.Status.Steps[i] = swarmv1alpha1.PipelineStepStatus{
			Name:  st.Name,
			Phase: swarmv1alpha1.PipelineStepPhasePending,
		}
		count++
	}
	return count
}

// getLatestSwarmRun fetches the most recently created SwarmRun for a team from the cluster.
func getLatestSwarmRun(ctx context.Context, teamName, namespace, kubeContext string) (*swarmv1alpha1.SwarmRun, error) {
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmrun", "-l", "kubeswarm.io/team="+teamName, "-o", "json")
	out, err := kubectlOutput(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("getting SwarmRuns for team %q: %w", teamName, err)
	}
	var list swarmv1alpha1.SwarmRunList
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, fmt.Errorf("parsing SwarmRuns: %w", err)
	}
	var latest *swarmv1alpha1.SwarmRun
	for i := range list.Items {
		run := &list.Items[i]
		if latest == nil || run.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = run
		}
	}
	return latest, nil
}

// createSwarmRun creates a new SwarmRun in the cluster by writing it to a temp file and applying it.
func createSwarmRun(ctx context.Context, run *swarmv1alpha1.SwarmRun, namespace, kubeContext string) error {
	data, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("marshaling SwarmRun: %w", err)
	}
	f, err := os.CreateTemp("", "swarmrun-*.json")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing SwarmRun: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "apply", "-f", f.Name())
	return shell(ctx, "kubectl", args...)
}

// patchSwarmRunStatus patches the status subresource of an SwarmRun.
func patchSwarmRunStatus(
	ctx context.Context, runName, namespace, kubeContext string, run *swarmv1alpha1.SwarmRun,
) error {
	type statusWrapper struct {
		Status swarmv1alpha1.SwarmRunStatus `json:"status"`
	}
	patch, err := json.Marshal(statusWrapper{Status: run.Status})
	if err != nil {
		return fmt.Errorf("marshaling status patch: %w", err)
	}
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "patch", "swarmrun", runName,
		"--subresource=status", "--type=merge", "-p", string(patch))
	return shell(ctx, "kubectl", args...)
}

// ─── swarm runs ────────────────────────────────────────────────────────────────

func runsCmd() *cobra.Command {
	var namespace string
	var kubeContext string

	cmd := &cobra.Command{
		Use:   "runs <team>",
		Short: "List SwarmRun history for a team",
		Long: `Show all SwarmRun records for a team, newest first.

Examples:
  swarm runs my-team -n my-namespace
  swarm runs my-team -n my-namespace --context prod-cluster`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRuns(cmd.Context(), args[0], namespace, kubeContext)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmTeam (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runRuns(ctx context.Context, teamName, namespace, kubeContext string) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmruns",
		"-l", "kubeswarm.io/team="+teamName,
		"--sort-by=.metadata.creationTimestamp")
	return shell(ctx, "kubectl", args...)
}

// ─── swarm operator ────────────────────────────────────────────────────────────

// operatorFlags are the shared flags for operator install/upgrade.
type operatorFlags struct {
	namespace    string
	version      string
	taskQueueURL string
	agentEnv     []string
	setValues    []string
	kubeContext  string
	wait         bool
}

func operatorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Manage the kubeswarm installation in a Kubernetes cluster",
		Long: `Install, upgrade, or inspect the kubeswarm Helm release.

Requires helm and kubectl to be installed and configured.`,
	}
	cmd.AddCommand(operatorInstallCmd())
	cmd.AddCommand(operatorUpgradeCmd())
	cmd.AddCommand(operatorStatusCmd())
	return cmd
}

func operatorInstallCmd() *cobra.Command {
	var f operatorFlags
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install kubeswarm via Helm",
		Long: `Add the kubeswarm Helm repo and install the kubeswarm chart.

Examples:
  swarm operator install
  swarm operator install --task-queue-url redis://redis.kubeswarm-system.svc:6379
  swarm operator install --agent-env OPENAI_BASE_URL=http://ollama:11434/v1 --agent-env OPENAI_API_KEY=ollama
  swarm operator install --version 0.3.5 --namespace kubeswarm-system`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHelmDeploy(cmd.Context(), "install", f)
		},
	}
	addOperatorFlags(cmd, &f)
	return cmd
}

func operatorUpgradeCmd() *cobra.Command {
	var f operatorFlags
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade (or install) the kubeswarm via Helm",
		Long: `Upgrade kubeswarm release, or install it if not present.

Examples:
  swarm operator upgrade
  swarm operator upgrade --version 0.3.5
  swarm operator upgrade --agent-env ANTHROPIC_API_KEY=sk-ant-...`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHelmDeploy(cmd.Context(), "upgrade", f)
		},
	}
	addOperatorFlags(cmd, &f)
	return cmd
}

func addOperatorFlags(cmd *cobra.Command, f *operatorFlags) {
	cmd.Flags().StringVar(&f.namespace, "namespace", "kubeswarm-system", "Kubernetes namespace for the operator")
	cmd.Flags().StringVar(&f.version, "version", "", "Chart version to install (default: latest)")
	cmd.Flags().StringVar(&f.taskQueueURL, "task-queue-url", "",
		"Redis connection string, e.g. redis://redis.kubeswarm-system.svc:6379")
	cmd.Flags().StringArrayVar(&f.agentEnv, "agent-env", nil,
		"Env var injected into every agent pod, e.g. OPENAI_API_KEY=sk-... (repeatable)")
	cmd.Flags().StringArrayVar(&f.setValues, "set", nil,
		"Pass-through --set to helm, e.g. --set replicaCount=2 (repeatable)")
	cmd.Flags().StringVar(&f.kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().BoolVar(&f.wait, "wait", false, "Wait for the operator pod to be ready before returning")
}

// runHelmDeploy executes `helm install` or `helm upgrade --install` for the kubeswarm.
func runHelmDeploy(ctx context.Context, mode string, f operatorFlags) error {
	if err := requireBinary("helm"); err != nil {
		return err
	}

	// Ensure the repo is present and up to date.
	fmt.Println("Updating Helm repo...")
	if err := shell(ctx, "helm", "repo", "add",
		"kubeswarm.io", "https://kubeswarm.github.io/kubeswarm-charts", "--force-update"); err != nil {
		return fmt.Errorf("helm repo add: %w", err)
	}
	if err := shell(ctx, "helm", "repo", "update", "kubeswarm.io"); err != nil {
		return fmt.Errorf("helm repo update: %w", err)
	}

	args := []string{mode}
	if mode == "upgrade" {
		args = append(args, "--install")
	}
	args = append(args, "kubeswarm", "kubeswarm/controller",
		"--namespace", f.namespace,
		"--create-namespace",
	)

	if f.version != "" {
		args = append(args, "--version", f.version)
	}
	if f.taskQueueURL != "" {
		args = append(args, "--set", "taskQueueURL="+f.taskQueueURL)
	}
	for _, kv := range f.setValues {
		args = append(args, "--set", kv)
	}
	if f.wait {
		args = append(args, "--wait")
	}
	if f.kubeContext != "" {
		args = append(args, "--kube-context", f.kubeContext)
	}

	// Build agentExtraEnv values file when --agent-env is provided.
	if len(f.agentEnv) > 0 {
		valuesFile, cleanup, err := buildAgentEnvValuesFile(f.agentEnv)
		if err != nil {
			return err
		}
		defer cleanup()
		args = append(args, "-f", valuesFile)
	}

	fmt.Printf("Running: helm %s\n", strings.Join(args, " "))
	return shell(ctx, "helm", args...)
}

// buildAgentEnvValuesFile writes a temporary Helm values file containing the
// agentExtraEnv array and returns the file path plus a cleanup function.
func buildAgentEnvValuesFile(envPairs []string) (path string, cleanup func(), err error) {
	type envEntry struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	var entries []envEntry
	for _, kv := range envPairs {
		before, after, ok := strings.Cut(kv, "=")
		if !ok {
			return "", nil, fmt.Errorf("--agent-env %q must be in NAME=VALUE format", kv)
		}
		entries = append(entries, envEntry{Name: before, Value: after})
	}

	var buf bytes.Buffer
	buf.WriteString("agentExtraEnv:\n")
	for _, e := range entries {
		buf.WriteString(fmt.Sprintf("  - name: %s\n    value: %q\n", e.Name, e.Value))
	}

	f, err := os.CreateTemp("", "kubeswarm-runtime-env-*.yaml")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp values file: %w", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", nil, err
	}
	_ = f.Close()
	return f.Name(), func() { _ = os.Remove(f.Name()) }, nil
}

func operatorStatusCmd() *cobra.Command {
	var kubeContext string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the kubeswarm installation status",
		Long: `Display the operator pod status and Helm release info.

Examples:
  swarm operator status
  swarm operator status --context my-cluster`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOperatorStatus(cmd.Context(), kubeContext)
		},
	}
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	return cmd
}

func runOperatorStatus(ctx context.Context, kubeContext string) error {
	if err := requireBinary("helm"); err != nil {
		return err
	}

	helmArgs := []string{"list", "--namespace", "kubeswarm-system", "--filter", "kubeswarm"}
	if kubeContext != "" {
		helmArgs = append(helmArgs, "--kube-context", kubeContext)
	}
	fmt.Println("=== Helm release ===")
	_ = shell(ctx, "helm", helmArgs...)

	fmt.Println("\n=== Operator pods ===")
	kubectlArgs := []string{"get", "pods", "--namespace", "kubeswarm-system", "-l", "app.kubernetes.io/name=kubeswarm"}
	if kubeContext != "" {
		kubectlArgs = append([]string{"--context", kubeContext}, kubectlArgs...)
	}
	_ = shell(ctx, "kubectl", kubectlArgs...)
	return nil
}

// ─── swarm deploy ──────────────────────────────────────────────────────────────

// kindPriority defines the apply order for kubeswarm resource kinds.
// Lower numbers are applied first. Unknown kinds default to -1 (apply before kubeswarm resources).
var kindPriority = map[string]int{
	"Namespace":     -2,
	"SwarmSettings": 0,
	"SwarmMemory":   1,
	"SwarmAgent":    2,
	"SwarmTeam":     3,
	"SwarmEvent":    4,
}

func deployCmd() *cobra.Command {
	var namespace string
	var kubeContext string
	var dryRun bool
	var wait bool

	cmd := &cobra.Command{
		Use:   "deploy <file>",
		Short: "Apply kubeswarm resources to a Kubernetes cluster",
		Long: `Parse a multi-document YAML file and apply resources in the correct order:
SwarmSettings -> SwarmMemory -> SwarmAgent -> SwarmTeam -> SwarmEvent.

The same YAML files that work with swarm run also work with swarm deploy.

Examples:
  swarm deploy my-agents.yaml
  swarm deploy my-agents.yaml --namespace my-team
  swarm deploy my-agents.yaml --context prod-cluster --dry-run
  swarm deploy my-agents.yaml --wait`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDeploy(cmd.Context(), args[0], namespace, kubeContext, dryRun, wait)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Override the namespace for all resources")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print what would be applied without sending to the cluster")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for SwarmAgent deployments to be ready after applying")
	return cmd
}

func runDeploy(ctx context.Context, path, namespace, kubeContext string, dryRun, wait bool) error {
	if !dryRun {
		if err := requireBinary("kubectl"); err != nil {
			return err
		}
	}

	data, err := os.ReadFile(path) //nolint:gosec // CLI reads a user-specified file path; intentional
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	// Resolve fileRef entries: read prompt files, rewrite as configMapKeyRef,
	// and collect the ConfigMaps to apply before the resources that reference them.
	rewritten, configMaps, err := swarm.ExtractFileRefs(data, filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("resolving fileRefs in %s: %w", path, err)
	}
	data = rewritten

	docs := swarm.SplitRawDocs(data)
	if len(docs) == 0 {
		return fmt.Errorf("no YAML documents found in %s", path)
	}

	// Prepend generated ConfigMaps. They sort before all Swarm resources (priority -1)
	// so they are always applied first.
	for _, cm := range configMaps {
		docs = append([]swarm.RawDoc{{Kind: "ConfigMap", Raw: cm.ToYAML()}}, docs...)
	}

	// Sort by kind priority; unknown kinds (priority -1) go first.
	sort.SliceStable(docs, func(i, j int) bool {
		pi, ok := kindPriority[docs[i].Kind]
		if !ok {
			pi = -1
		}
		pj, ok := kindPriority[docs[j].Kind]
		if !ok {
			pj = -1
		}
		return pi < pj
	})

	// Show what we found.
	kindCounts := make(map[string]int)
	for _, d := range docs {
		k := d.Kind
		if k == "" {
			k = "<unknown>"
		}
		kindCounts[k]++
	}
	fmt.Printf("Found %d resource(s):\n", len(docs))
	for k, n := range kindCounts {
		fmt.Printf("  %-20s %d\n", k, n)
	}
	fmt.Println()

	if dryRun {
		fmt.Println("(dry-run — not applying)")
		return nil
	}

	// Build the sorted YAML and pipe it to kubectl.
	var combined bytes.Buffer
	for i, d := range docs {
		if i > 0 {
			combined.WriteString("\n---\n")
		}
		combined.Write(d.Raw)
	}

	args := []string{"apply", "-f", "-"}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	if kubeContext != "" {
		args = append([]string{"--context", kubeContext}, args...)
	}

	fmt.Printf("Applying %d resource(s)...\n", len(docs))
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = &combined
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl apply: %w", err)
	}

	if wait {
		return waitForAgents(ctx, namespace, kubeContext)
	}
	return nil
}

// waitForAgents waits for all SwarmAgent-managed deployments to roll out.
func waitForAgents(ctx context.Context, namespace, kubeContext string) error {
	fmt.Println("\nWaiting for SwarmAgent deployments to be ready...")

	args := []string{"rollout", "status", "deployment", "--selector", "kubeswarm.io/kind=SwarmAgent", "--timeout", "120s"}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	} else {
		args = append(args, "--all-namespaces")
	}
	if kubeContext != "" {
		args = append([]string{"--context", kubeContext}, args...)
	}
	return shell(ctx, "kubectl", args...)
}

// ─── swarm status ──────────────────────────────────────────────────────────────

func statusCmd() *cobra.Command {
	var namespace string
	var kubeContext string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the status of kubeswarm resources in a cluster",
		Long: `Display a summary of running kubeswarm resources: operator health,
SwarmAgents, SwarmTeams, SwarmRuns, and SwarmEvents.

Examples:
  swarm status
  swarm status --namespace my-team
  swarm status --all-namespaces
  swarm status --context prod-cluster`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStatus(cmd.Context(), namespace, kubeContext, allNamespaces)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to inspect (default: current context namespace)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Show resources across all namespaces")
	return cmd
}

func runStatus(ctx context.Context, namespace, kubeContext string, allNamespaces bool) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	base := buildKubectlBase(kubeContext, namespace, allNamespaces)

	fmt.Println("=== Operator ===")
	opArgs := append(base, "get", "pods", "--namespace", "kubeswarm-system",
		"-l", "app.kubernetes.io/name=kubeswarm",
		"--no-headers", "-o", "wide")
	_ = shell(ctx, "kubectl", opArgs...)

	sections := []struct {
		label    string
		resource string
	}{
		{"SwarmAgents", "swarmagents"},
		{"SwarmTeams", "swarmteams"},
		{"SwarmRuns", "swarmruns"},
		{"SwarmEvents", "swarmevents"},
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, s := range sections {
		args := append(base, "get", s.resource)
		out, err := kubectlOutput(ctx, args...)
		if err != nil {
			// CRD not installed — skip silently.
			continue
		}
		if strings.TrimSpace(out) == "" || strings.Contains(out, "No resources found") {
			continue
		}
		_, _ = fmt.Fprintf(w, "\n=== %s ===\n", s.label)
		_, _ = fmt.Fprintln(w, out)
	}
	_ = w.Flush()
	return nil
}

func buildKubectlBase(kubeContext, namespace string, allNamespaces bool) []string {
	var args []string
	if kubeContext != "" {
		args = append(args, "--context", kubeContext)
	}
	switch {
	case allNamespaces:
		args = append(args, "--all-namespaces")
	case namespace != "":
		args = append(args, "--namespace", namespace)
	}
	return args
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// requireBinary checks that a binary is available in PATH.
func requireBinary(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%q not found in PATH — install it and try again", name)
	}
	return nil
}

// shell runs an external command with its stdout/stderr wired to the terminal.
func shell(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// kubectlOutput runs kubectl and returns its stdout as a string.
func kubectlOutput(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	return string(out), err
}

// ─── swarm registry ─────────────────────────────────────────────────────────────

func registryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "Inspect the SwarmRegistry capability index",
		Long:  `Commands for exploring the capability index maintained by SwarmRegistry.`,
	}
	cmd.AddCommand(registryListCmd())
	cmd.AddCommand(registryQueryCmd())
	cmd.AddCommand(registryTraceCmd())
	return cmd
}

func registryListCmd() *cobra.Command {
	var namespace, kubeContext, registryName string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all capabilities indexed by a registry",
		Long: `Print every capability ID known to the SwarmRegistry, with the agents
that advertise it and the tags associated with each.

Examples:
  swarm registry list -n my-namespace
  swarm registry list -n my-namespace --registry platform-registry`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRegistryList(cmd.Context(), namespace, kubeContext, registryName)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmRegistry (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&registryName, "registry", "", "Registry name (defaults to the first registry found)")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runRegistryList(ctx context.Context, namespace, kubeContext, registryName string) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	// Resolve registry name if not specified.
	if registryName == "" {
		args := buildKubectlBase(kubeContext, namespace, false)
		args = append(args, "get", "swarmregistries", "-o", "jsonpath={.items[0].metadata.name}")
		name, err := kubectlOutput(ctx, args...)
		if err != nil || strings.TrimSpace(name) == "" {
			return fmt.Errorf("no SwarmRegistry found in namespace %q", namespace)
		}
		registryName = strings.TrimSpace(name)
	}

	// Fetch status.capabilities from the registry.
	capJSONPath := "{range .status.capabilities[*]}" +
		"{.id}\\t{range .agents[*]}{@}{','}{end}\\t{range .tags[*]}{@}{','}{end}\\n{end}"
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmregistry", registryName, "-o", "jsonpath="+capJSONPath)
	raw, err := kubectlOutput(ctx, args...)
	if err != nil {
		return err
	}

	if strings.TrimSpace(raw) == "" {
		fmt.Printf("registry %q has no indexed capabilities\n", registryName)
		return nil
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CAPABILITY\tAGENTS\tTAGS"); err != nil {
		return err
	}
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 3 {
			continue
		}
		capID := parts[0]
		agents := strings.TrimRight(parts[1], ",")
		tags := strings.TrimRight(parts[2], ",")
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", capID, agents, tags); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func registryQueryCmd() *cobra.Command {
	var namespace, kubeContext, registryName string
	var tags []string

	cmd := &cobra.Command{
		Use:   "query <capability-id>",
		Short: "Find agents that match a capability",
		Long: `Show which agents in the registry advertise the given capability.
Use --tags to narrow the results to agents that also declare all listed tags.

Examples:
  swarm registry query review-contract -n my-namespace
  swarm registry query review-contract --tags legal,contract -n my-namespace`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRegistryQuery(cmd.Context(), args[0], tags, namespace, kubeContext, registryName)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmRegistry (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&registryName, "registry", "", "Registry name (defaults to the first registry found)")
	cmd.Flags().StringSliceVar(&tags, "tags", nil, "Require agents to have all listed tags (comma-separated)")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runRegistryQuery(
	ctx context.Context, capID string, tags []string, namespace, kubeContext, registryName string,
) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	if registryName == "" {
		args := buildKubectlBase(kubeContext, namespace, false)
		args = append(args, "get", "swarmregistries", "-o", "jsonpath={.items[0].metadata.name}")
		name, err := kubectlOutput(ctx, args...)
		if err != nil || strings.TrimSpace(name) == "" {
			return fmt.Errorf("no SwarmRegistry found in namespace %q", namespace)
		}
		registryName = strings.TrimSpace(name)
	}

	capJSONPath2 := "{range .status.capabilities[*]}" +
		"{.id}\\t{range .agents[*]}{@}{','}{end}\\t{range .tags[*]}{@}{','}{end}\\n{end}"
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmregistry", registryName, "-o", "jsonpath="+capJSONPath2)
	raw, err := kubectlOutput(ctx, args...)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CAPABILITY\tAGENTS\tTAGS"); err != nil {
		return err
	}
	found := false
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 3 || parts[0] != capID {
			continue
		}
		// Tag filter.
		if len(tags) > 0 {
			lineTags := strings.Split(strings.TrimRight(parts[2], ","), ",")
			tagSet := make(map[string]struct{}, len(lineTags))
			for _, t := range lineTags {
				tagSet[strings.TrimSpace(t)] = struct{}{}
			}
			match := true
			for _, required := range tags {
				if _, ok := tagSet[strings.TrimSpace(required)]; !ok {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		agents := strings.TrimRight(parts[1], ",")
		lineTags := strings.TrimRight(parts[2], ",")
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", parts[0], agents, lineTags); err != nil {
			return err
		}
		found = true
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if !found {
		fmt.Printf("no agents found for capability %q\n", capID)
	}
	return nil
}

func registryTraceCmd() *cobra.Command {
	var namespace, kubeContext string

	cmd := &cobra.Command{
		Use:   "trace <run-name>",
		Short: "Show registry-resolved agents for each step in a run",
		Long: `Print the agent selected by a registry lookup for each step in an SwarmRun.
Steps that used static role references are shown with an empty resolved-agent column.

Examples:
  swarm registry trace my-run-abc123 -n my-namespace`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRegistryTrace(cmd.Context(), args[0], namespace, kubeContext)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmRun (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runRegistryTrace(ctx context.Context, runName, namespace, kubeContext string) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmrun", runName,
		"-o", "jsonpath={range .status.steps[*]}{.name}\\t{.phase}\\t{.resolvedAgent}\\n{end}")
	raw, err := kubectlOutput(ctx, args...)
	if err != nil {
		return err
	}

	if strings.TrimSpace(raw) == "" {
		fmt.Printf("run %q has no steps recorded yet\n", runName)
		return nil
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "STEP\tPHASE\tRESOLVED AGENT"); err != nil {
		return err
	}
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 2 {
			continue
		}
		step := parts[0]
		phase := parts[1]
		resolved := ""
		if len(parts) == 3 {
			resolved = parts[2]
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", step, phase, resolved); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// ─── swarm mcp ──────────────────────────────────────────────────────────────────

// ─── swarm mcp ──────────────────────────────────────────────────────────────────

// mcpCmd is the parent for MCP-gateway subcommands.
func mcpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Inspect MCP-exposed agents and their tools",
		Long:  `Commands for exploring agents that expose capabilities via the MCP gateway.`,
	}
	cmd.AddCommand(mcpListCmd())
	return cmd
}

// mcpListCmd lists all SwarmAgents that expose at least one MCP capability, together
// with the exposed tool names read from status.exposedMCPCapabilities.
//
// Examples:
//
//	swarm mcp list -n ai-team
//	swarm mcp list -n ai-team --agent database-agent
func mcpListCmd() *cobra.Command {
	var namespace, kubeContext, agentName string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List agents with MCP-exposed capabilities",
		Long: `Print every SwarmAgent that exposes at least one capability via the MCP gateway,
along with the tool names readable from status.exposedMCPCapabilities.

Examples:
  swarm mcp list -n my-namespace
  swarm mcp list -n my-namespace --agent database-agent`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runMCPList(cmd.Context(), namespace, kubeContext, agentName)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to query (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&agentName, "agent", "", "Limit output to a single agent")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runMCPList(ctx context.Context, namespace, kubeContext, agentName string) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	// Build the jsonpath that prints: <name> TAB <comma-separated tools> NEWLINE
	// for every agent that has at least one exposed MCP capability.
	jsonPathExpr := "{range .items[*]}" +
		"{.metadata.name}\\t" +
		"{range .status.exposedMCPCapabilities[*]}{@}{','}{end}\\n" +
		"{end}"

	args := buildKubectlBase(kubeContext, namespace, false)
	if agentName != "" {
		// Single-agent mode: get the specific agent object directly.
		jsonPathExpr = "{.metadata.name}\\t{range .status.exposedMCPCapabilities[*]}{@}{','}{end}\\n"
		args = append(args, "get", "swarmagent", agentName, "-o", "jsonpath="+jsonPathExpr)
	} else {
		args = append(args, "get", "swarmagents", "-o", "jsonpath="+jsonPathExpr)
	}

	raw, err := kubectlOutput(ctx, args...)
	if err != nil {
		return err
	}

	// Filter out agents with no exposed capabilities.
	var rows []string
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 2 {
			continue
		}
		tools := strings.TrimRight(strings.TrimSpace(parts[1]), ",")
		if tools == "" {
			continue // agent has no exposed MCP capabilities
		}
		rows = append(rows, parts[0]+"\t"+tools)
	}

	if len(rows) == 0 {
		if agentName != "" {
			fmt.Printf("agent %q exposes no MCP capabilities\n", agentName)
		} else {
			fmt.Printf("no agents in namespace %q expose MCP capabilities\n", namespace)
		}
		return nil
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "AGENT\tEXPOSED TOOLS"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(tw, row); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// ─── swarm agent ────────────────────────────────────────────────────────────────

// agentCmd is the parent for standalone SwarmAgent subcommands.
func agentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Interact with standalone SwarmAgents",
		Long:  `Commands for sending tasks to standalone SwarmAgents via the MCP gateway.`,
	}
	cmd.AddCommand(agentSubmitCmd())
	return cmd
}

// agentSubmitCmd sends a prompt to a standalone SwarmAgent via the MCP gateway
// and prints the response. It port-forwards to the gateway automatically.
//
// Examples:
//
//	swarm agent submit analyst-agent "Which customers have shipped orders?" -n kubeswarm-teams
func agentSubmitCmd() *cobra.Command {
	var namespace, kubeContext, capability string
	var gatewayPort int

	cmd := &cobra.Command{
		Use:   "submit <agent> <prompt>",
		Short: "Send a prompt to a standalone SwarmAgent via the MCP gateway",
		Long: `Send a prompt to a standalone SwarmAgent through the built-in MCP gateway
and print the response. The gateway is port-forwarded automatically.

The agent must have at least one capability with exposeMCP: true.
Use --capability to target a specific capability; defaults to the first exposed one.

Examples:
  swarm agent submit analyst-agent "Which customers have shipped orders?" -n kubeswarm-teams
  swarm agent submit database-agent "Describe the schema" -n kubeswarm-teams --capability describe_schema`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentSubmit(cmd.Context(), args[0], args[1], capability, namespace, kubeContext, gatewayPort)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace of the SwarmAgent (required)")
	cmd.Flags().StringVar(&kubeContext, "context", "", "Kubernetes context to use")
	cmd.Flags().StringVar(&capability, "capability", "", "Capability ID to invoke (defaults to first exposed)")
	cmd.Flags().IntVar(&gatewayPort, "gateway-port", 8093, "Local port for MCP gateway port-forward")
	_ = cmd.MarkFlagRequired("namespace")
	return cmd
}

func runAgentSubmit(
	ctx context.Context, agentName, prompt, capability, namespace, kubeContext string, gatewayPort int,
) error {
	if err := requireBinary("kubectl"); err != nil {
		return err
	}

	// Resolve capability if not specified — use the first exposed one.
	if capability == "" {
		cap, err := resolveFirstExposedCapability(ctx, agentName, namespace, kubeContext)
		if err != nil {
			return err
		}
		capability = cap
	}

	// Start port-forward to the MCP gateway in the background.
	pfArgs := buildKubectlBase(kubeContext, "kubeswarm-system", false)
	pfArgs = append(pfArgs, "port-forward",
		"svc/kubeswarm", fmt.Sprintf("%d:8093", gatewayPort),
	)
	pfCmd := exec.CommandContext(ctx, "kubectl", pfArgs...) //nolint:gosec
	pfCmd.Stderr = io.Discard
	if err := pfCmd.Start(); err != nil {
		return fmt.Errorf("starting port-forward: %w", err)
	}
	defer pfCmd.Process.Kill() //nolint:errcheck

	// Give the port-forward a moment to establish.
	time.Sleep(800 * time.Millisecond)

	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	// Open an MCP SSE session.
	sseURL := fmt.Sprintf("%s/namespaces/%s/agents/%s/sse", baseURL, namespace, agentName)
	sseResp, err := http.Get(sseURL) //nolint:noctx,gosec
	if err != nil {
		return fmt.Errorf("connecting to MCP gateway: %w", err)
	}
	defer sseResp.Body.Close() //nolint:errcheck

	// Read the endpoint event to get the sessionId.
	var messageEndpoint string
	scanner := bufio.NewScanner(sseResp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if endpoint, ok := strings.CutPrefix(line, "data: "); ok {
			messageEndpoint = endpoint
			break
		}
	}
	if messageEndpoint == "" {
		return fmt.Errorf("no endpoint received from MCP gateway SSE")
	}

	postURL := baseURL + messageEndpoint

	// Helper: send a JSON-RPC request and read the SSE response.
	nextID := 0
	call := func(method string, params any) (json.RawMessage, error) {
		nextID++
		body, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      nextID,
			"method":  method,
			"params":  params,
		})
		resp, err := http.Post(postURL, "application/json", bytes.NewReader(body)) //nolint:noctx,gosec
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()

		// Read the response off the SSE stream.
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var rpc struct {
					Result json.RawMessage `json:"result"`
					Error  *struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &rpc); err != nil {
					continue
				}
				if rpc.Error != nil {
					return nil, fmt.Errorf("MCP error: %s", rpc.Error.Message)
				}
				return rpc.Result, nil
			}
		}
		return nil, fmt.Errorf("SSE stream ended without response")
	}

	// MCP handshake.
	if _, err := call("initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"clientInfo":      map[string]any{"name": "cli", "version": version},
		"capabilities":    map[string]any{},
	}); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	// Send initialized notification (no response expected).
	notif, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	})
	http.Post(postURL, "application/json", bytes.NewReader(notif)) //nolint:errcheck,noctx,gosec

	// Call the tool.
	result, err := call("tools/call", map[string]any{
		"name":      capability,
		"arguments": map[string]any{"query": prompt},
	})
	if err != nil {
		return err
	}

	// Parse and print the tool response.
	var toolResult struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result, &toolResult); err != nil {
		fmt.Println(string(result))
		return nil
	}
	for _, c := range toolResult.Content {
		fmt.Println(c.Text)
	}
	if toolResult.IsError {
		return fmt.Errorf("agent returned an error")
	}
	return nil
}

// --- swarm audit ----------------------------------------------------------------

// auditFlags holds the parsed flags for the audit subcommand.
type auditFlags struct {
	agent     string
	action    string
	run       string
	namespace string
	status    string
	since     string
	output    string
	redisURL  string
	limit     int64
}

func auditCmd() *cobra.Command {
	var f auditFlags

	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Query the agent audit trail",
		Long: `Read audit events from a Redis Stream and display them.

Requires the Redis audit sink to be enabled (see RFC-0030).
Set --redis-url or the KUBESWARM_REDIS_URL environment variable.

Examples:
  swarm audit --agent report-coordinator --since 1h
  swarm audit --action tool.called --since 24h
  swarm audit --run fanout-test-4
  swarm audit --namespace production --status error --since 7d
  swarm audit --agent report-coordinator --since 1h --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAudit(cmd.Context(), f)
		},
	}

	defaultRedisURL := os.Getenv("KUBESWARM_REDIS_URL")

	cmd.Flags().StringVar(&f.agent, "agent", "", "Filter by agent name")
	cmd.Flags().StringVar(&f.action, "action", "", "Filter by action type (e.g., tool.called, task.completed)")
	cmd.Flags().StringVar(&f.run, "run", "", "Filter by run ID")
	cmd.Flags().StringVarP(&f.namespace, "namespace", "n", "default", "Filter by namespace")
	cmd.Flags().StringVar(&f.status, "status", "", "Filter by status (success, error, timeout, denied)")
	cmd.Flags().StringVar(&f.since, "since", "", "Only show events from the last N duration (e.g., 1h, 24h, 7d)")
	cmd.Flags().StringVarP(&f.output, "output", "o", "table", "Output format: table or json")
	cmd.Flags().StringVar(&f.redisURL, "redis-url", defaultRedisURL,
		"Redis connection string (or set KUBESWARM_REDIS_URL)")
	cmd.Flags().Int64Var(&f.limit, "limit", 100, "Max number of events to return")

	cmd.AddCommand(auditTreeCmd(&f))
	return cmd
}

func auditTreeCmd(parentFlags *auditFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tree <eventId>",
		Short: "Display the causal chain for an audit event",
		Long: `Traverse the parentEventId linkage to reconstruct the full execution tree.

Finds the root event by walking up the parent chain, then displays all
descendants as an indented tree.

Examples:
  swarm audit tree evt-abc123
  swarm audit tree evt-abc123 --redis-url redis://localhost:6379`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAuditTree(cmd.Context(), args[0], *parentFlags)
		},
	}
	return cmd
}

// parseSinceDuration parses a duration string that supports 'd' for days
// in addition to standard Go durations (h, m, s).
func parseSinceDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	// Handle 'd' suffix for days.
	if before, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.Atoi(before)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// sinceToStreamID converts a --since duration to a Redis Stream minimum ID
// (millisecond timestamp).
func sinceToStreamID(since string) (string, error) {
	if since == "" {
		return "-", nil // no lower bound
	}
	d, err := parseSinceDuration(since)
	if err != nil {
		return "", err
	}
	ts := time.Now().Add(-d).UnixMilli()
	return fmt.Sprintf("%d-0", ts), nil
}

// connectRedis creates a Redis client from a URL string.
func connectRedis(redisURL string) (*redis.Client, error) {
	if redisURL == "" {
		return nil, fmt.Errorf(
			"no Redis URL configured - set --redis-url or KUBESWARM_REDIS_URL\n\n" +
				"The audit CLI requires the Redis audit sink. " +
				"See: swarm operator install --set auditLog.sink=redis")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL %q: %w", redisURL, err)
	}
	return redis.NewClient(opts), nil
}

// streamKey returns the Redis Stream key for a namespace.
func streamKey(namespace string) string {
	return "kubeswarm:audit:" + namespace
}

// parseAuditEvent unmarshals an AuditEvent from a Redis Stream message.
func parseAuditEvent(values map[string]any) (audit.AuditEvent, error) {
	var evt audit.AuditEvent
	data, ok := values["data"]
	if !ok {
		return evt, fmt.Errorf("stream message missing 'data' field")
	}
	s, ok := data.(string)
	if !ok {
		return evt, fmt.Errorf("stream message 'data' field is not a string")
	}
	if err := json.Unmarshal([]byte(s), &evt); err != nil {
		return evt, fmt.Errorf("unmarshaling audit event: %w", err)
	}
	return evt, nil
}

// matchesFilters returns true if the event passes all active filters.
func matchesFilters(evt audit.AuditEvent, f auditFlags) bool {
	if f.agent != "" && evt.Agent != f.agent {
		return false
	}
	if f.action != "" && string(evt.Action) != f.action {
		return false
	}
	if f.run != "" && evt.RunID != f.run {
		return false
	}
	if f.status != "" && string(evt.Status) != f.status {
		return false
	}
	return true
}

// extractDurationMs extracts detail.durationMs or detail.totalDurationMs from the event detail.
func extractDurationMs(detail json.RawMessage) string {
	if len(detail) == 0 {
		return "-"
	}
	var d struct {
		DurationMs      *int64 `json:"durationMs"`
		TotalDurationMs *int64 `json:"totalDurationMs"`
	}
	if err := json.Unmarshal(detail, &d); err != nil {
		return "-"
	}
	if d.DurationMs != nil {
		return fmt.Sprintf("%dms", *d.DurationMs)
	}
	if d.TotalDurationMs != nil {
		return fmt.Sprintf("%dms", *d.TotalDurationMs)
	}
	return "-"
}

func runAudit(ctx context.Context, f auditFlags) error {
	rdb, err := connectRedis(f.redisURL)
	if err != nil {
		return err
	}
	defer rdb.Close() //nolint:errcheck

	minID, err := sinceToStreamID(f.since)
	if err != nil {
		return fmt.Errorf("invalid --since value: %w", err)
	}

	// XREVRANGE returns newest first.
	// Fetch more than the limit to account for client-side filtering.
	fetchCount := max(f.limit*3, 300)

	key := streamKey(f.namespace)
	msgs, err := rdb.XRevRangeN(ctx, key, "+", minID, fetchCount).Result()
	if err != nil {
		return fmt.Errorf("reading audit stream %q: %w", key, err)
	}

	var events []audit.AuditEvent
	for _, msg := range msgs {
		if int64(len(events)) >= f.limit {
			break
		}
		evt, err := parseAuditEvent(msg.Values)
		if err != nil {
			continue // skip malformed events
		}
		if !matchesFilters(evt, f) {
			continue
		}
		events = append(events, evt)
	}

	if len(events) == 0 {
		fmt.Println("No audit events found.")
		if f.since != "" {
			fmt.Printf("Tip: try a wider time range (current: --since %s)\n", f.since)
		}
		return nil
	}

	switch f.output {
	case outputFormatJSON:
		return printAuditJSON(events)
	default:
		printAuditTable(events)
		return nil
	}
}

func printAuditJSON(events []audit.AuditEvent) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(events)
}

func printAuditTable(events []audit.AuditEvent) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TIMESTAMP\tACTION\tAGENT\tRUN/TASK\tMODEL\tTOKENS\tDURATION\tSTATUS") //nolint:errcheck
	for _, evt := range events {
		dur := extractDurationMs(evt.Detail)

		// Run or task ID for context.
		ref := evt.TaskID
		if evt.RunID != "" {
			ref = evt.RunID
		}
		if len(ref) > 20 {
			ref = ref[:20] + ".."
		}

		// Token summary.
		tokens := "-"
		if evt.Tokens != nil && (evt.Tokens.Input > 0 || evt.Tokens.Output > 0) {
			tokens = fmt.Sprintf("%d/%d", evt.Tokens.Input, evt.Tokens.Output)
		}

		// Model (short).
		model := evt.Model
		if model == "" {
			model = "-"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", //nolint:errcheck
			evt.Timestamp,
			string(evt.Action),
			evt.Agent,
			ref,
			model,
			tokens,
			dur,
			string(evt.Status),
		)
	}
	w.Flush() //nolint:errcheck
}

// --- swarm audit tree --------------------------------------------------------

func runAuditTree(ctx context.Context, eventID string, f auditFlags) error {
	rdb, err := connectRedis(f.redisURL)
	if err != nil {
		return err
	}
	defer rdb.Close() //nolint:errcheck

	key := streamKey(f.namespace)

	// Load all events from the stream into memory for tree traversal.
	// For very large streams, we limit to a reasonable scan window.
	allEvents, err := loadAllEvents(ctx, rdb, key)
	if err != nil {
		return fmt.Errorf("loading audit events: %w", err)
	}

	if len(allEvents) == 0 {
		return fmt.Errorf("no audit events found in stream %q", key)
	}

	// Build lookup maps.
	byID := make(map[string]audit.AuditEvent, len(allEvents))
	children := make(map[string][]audit.AuditEvent)
	for _, evt := range allEvents {
		byID[evt.EventID] = evt
		if evt.ParentEventID != "" {
			children[evt.ParentEventID] = append(children[evt.ParentEventID], evt)
		}
	}

	// Find the target event.
	target, ok := byID[eventID]
	if !ok {
		return fmt.Errorf("event %q not found in stream %q", eventID, key)
	}

	// Walk up to find root.
	root := target
	for root.ParentEventID != "" {
		parent, ok := byID[root.ParentEventID]
		if !ok {
			break // parent not in stream, treat current as root
		}
		root = parent
	}

	// Sort children by timestamp for consistent display.
	for k := range children {
		sort.Slice(children[k], func(i, j int) bool {
			return children[k][i].Timestamp < children[k][j].Timestamp
		})
	}

	// Print tree from root.
	printEventTree(root, children, 0)
	return nil
}

// loadAllEvents reads all messages from a Redis Stream using XRANGE.
// It caps at 10000 events to avoid unbounded memory usage.
func loadAllEvents(ctx context.Context, rdb *redis.Client, key string) ([]audit.AuditEvent, error) {
	const maxScan = 10000
	msgs, err := rdb.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		return nil, err
	}

	events := make([]audit.AuditEvent, 0, len(msgs))
	for i, msg := range msgs {
		if i >= maxScan {
			break
		}
		evt, err := parseAuditEvent(msg.Values)
		if err != nil {
			continue
		}
		events = append(events, evt)
	}
	return events, nil
}

func printEventTree(evt audit.AuditEvent, children map[string][]audit.AuditEvent, depth int) {
	indent := strings.Repeat("  ", depth)
	dur := extractDurationMs(evt.Detail)

	line := fmt.Sprintf("%s%s %s %s [%s]",
		indent,
		evt.EventID,
		string(evt.Action),
		evt.Agent,
		string(evt.Status),
	)
	if dur != "-" {
		line += " " + dur
	}

	// For delegate.sent, show target role.
	if evt.Action == audit.ActionDelegateSent && len(evt.Detail) > 0 {
		var d struct {
			ToRole string `json:"toRole"`
		}
		if json.Unmarshal(evt.Detail, &d) == nil && d.ToRole != "" {
			// Insert "-> toRole" after the action.
			line = fmt.Sprintf("%s%s %s -> %s %s [%s]",
				indent,
				evt.EventID,
				string(evt.Action),
				d.ToRole,
				evt.Agent,
				string(evt.Status),
			)
			if dur != "-" {
				line += " " + dur
			}
		}
	}

	fmt.Println(line)

	for _, child := range children[evt.EventID] {
		printEventTree(child, children, depth+1)
	}
}

// resolveFirstExposedCapability fetches the SwarmAgent and returns the first
// capability with exposeMCP: true.
func resolveFirstExposedCapability(ctx context.Context, agentName, namespace, kubeContext string) (string, error) {
	args := buildKubectlBase(kubeContext, namespace, false)
	args = append(args, "get", "swarmagent", agentName,
		"-o", "jsonpath={.status.exposedMCPCapabilities[0]}")
	cap, err := kubectlOutput(ctx, args...)
	if err != nil {
		return "", fmt.Errorf("fetching SwarmAgent %q: %w", agentName, err)
	}
	cap = strings.TrimSpace(cap)
	if cap == "" {
		return "", fmt.Errorf("agent %q has no exposed MCP capabilities — add exposeMCP: true to a capability", agentName)
	}
	return cap, nil
}

<p align="center">
  <img src="https://assets.kubeswarm.io/logo.svg" width="200" alt="kubeswarm">
</p>

<h3 align="center">kubeswarm - cli</h3>

<p align="center">
  <a href="https://github.com/kubeswarm/kubeswarm-cli/actions/workflows/ci.yml"><img src="https://github.com/kubeswarm/kubeswarm-cli/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/kubeswarm/kubeswarm-cli/releases"><img src="https://img.shields.io/github/v/release/kubeswarm/kubeswarm-cli" alt="GitHub release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
  <a href="go.mod"><img src="https://img.shields.io/github/go-mod/go-version/kubeswarm/kubeswarm-cli" alt="Go version"></a>
</p>

Local development CLI for [kubeswarm](https://github.com/kubeswarm/kubeswarm) - run, test and debug AI agent pipelines without a Kubernetes cluster.

Uses the same YAML files as `kubectl apply`. No cluster required.

Full documentation at **[docs.kubeswarm.io](https://docs.kubeswarm.io)**.

## Install

```bash
go install github.com/kubeswarm/kubeswarm-cli/cmd/swarm@latest
```

Or download a pre-built binary from the [releases page](https://github.com/kubeswarm/kubeswarm-cli/releases).

## Quick start

```bash
# Run a pipeline locally with a real LLM
swarm run my-team.yaml

# Run with mock responses (no API key needed)
swarm run my-team.yaml --provider mock

# Validate YAML without running
swarm validate my-team.yaml

# Stream step-by-step output
swarm run my-team.yaml --watch

# Machine-readable output for CI
swarm run my-team.yaml --output json
```

## Commands

| Command | Description |
|---|---|
| `swarm run <file>` | Execute an SwarmTeam pipeline locally |
| `swarm run --provider mock` | Run with fake LLM responses |
| `swarm run --dry-run` | Validate YAML and estimate token cost |
| `swarm run --watch` | Stream step-by-step output inline |
| `swarm run --no-mcp` | Skip MCP tool connections |
| `swarm run --output json` | Machine-readable output for CI |
| `swarm validate <file>` | Validate YAML structure and DAG |
| `swarm operator install` | Install kubeswarm into a cluster via Helm |
| `swarm operator upgrade` | Upgrade an existing kubeswarm installation |
| `swarm trigger <team>` | Trigger a team pipeline in-cluster |
| `swarm status` | Show status of running pipelines |
| `swarm logs <run>` | Tail logs from a running pipeline |

## Providers

| Flag | Description |
|---|---|
| `--provider auto` | Auto-detect from model name (default) |
| `--provider anthropic` | Force Anthropic Claude |
| `--provider openai` | Force OpenAI / compatible endpoint |
| `--provider mock` | Fake responses - no API key needed |

Set `OPENAI_BASE_URL` to use any OpenAI-compatible endpoint (Ollama, vLLM, etc.).

## Example output

```
Step "research"   [running]  ──────────────────── 4.2s
Step "research"   [done]     1,204 tokens  2.1s
  └─ "Here are 3 key findings about Kubernetes operator patterns..."

Step "summarize"  [done]     312 tokens   1.8s
  └─ "• Operators extend the Kubernetes API via CRDs..."

Run succeeded in 9.1s - total: 1,516 tokens
```

## Related

- [kubeswarm](https://github.com/kubeswarm/kubeswarm) - the Kubernetes operator
- [cookbook](https://github.com/kubeswarm/kubeswarm-cookbook) - example pipelines
- [helm-charts](https://github.com/kubeswarm/helm-charts) - Helm chart for cluster deployment

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Apache 2.0 - see [LICENSE](LICENSE).

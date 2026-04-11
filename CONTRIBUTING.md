# Contributing to kubeswarm

Thank you for your interest in contributing. This document covers the development workflow and standards for the swarm CLI.

## Before you start

- **Open an issue first** for any non-trivial change (new command, behavior change, breaking flag). Bug fixes and docs can go straight to a PR.

## Development setup

**Prerequisites**

| Tool | Version          |
|------|------------------|
| Go   | 1.26.2 (see `go.mod`) |

```bash
git clone https://github.com/kubeswarm/kubeswarm-cli.git
cd kubeswarm-cli
make setup
make ci
```

## Project layout

```
cmd/swarm/main.go              # CLI entrypoint (cobra)
internal/swarm/
  loader.go              # Parse multi-doc YAML -> SwarmTeam + SwarmAgent
  local/executor.go      # Drive pipeline steps locally (no Redis, no cluster)
```

The CLI reuses `pkg/agent/providers/` from kubeswarm for LLM calls - no logic is duplicated.

## Adding a command

1. Add a `cobra.Command` under `cmd/swarm/main.go` or a new file in `cmd/`.
2. Wire it into the root command.
3. Add a test that exercises the command with `--provider mock` (no API key needed).
4. Update the commands table in `README.md`.

## Testing locally

```bash
# Run a pipeline with fake LLM responses - no API key needed
go run ./cmd/swarm run quickstart.yaml --provider mock

# Validate YAML without running
go run ./cmd/swarm/main.go validate quickstart.yaml

# Stream output
go run ./cmd/swarm/main.go run quickstart.yaml --provider mock --watch
```

## Branch naming

Name your branch with one of these prefixes - a GitHub Action will automatically label the PR:

| Prefix | Label | Example |
| --- | --- | --- |
| `feat/` | `feat` | `feat/add-watch-flag` |
| `fix/` | `bug` | `fix/mock-provider-panic` |
| `docs/` | `docs` | `docs/update-commands-table` |
| `chore/` | `chore` | `chore/bump-go-version` |

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/). The prefix must match the branch prefix:

```
<type>: <short description>
```

Allowed types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`

Keep the first line under 72 characters.

## Code style

- Run `make lint` before submitting - `make lint` runs `gofmt` automatically.
- Flags must have short descriptions (shown in `--help`).
- `--output json` support is required for any command that produces structured output.
- Do not import `k8s.io/*` or `sigs.k8s.io/controller-runtime` - the CLI must work without a cluster.

## Security practices

- **Pin exact versions** in `go.mod`. Never run `go get package@latest` and commit without verifying the resolved version.
- **No secrets in code or defaults** - API keys are read from environment variables only (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`). Default values must be empty string.
- **No `fmt.Sprintf` in exec calls** - pass arguments as separate strings to `exec.Command`, never interpolate into a command string.
- **Validate file paths** - reject paths with `..` traversal when loading YAML files.
- Run `go mod tidy` before committing so `go.sum` stays minimal.

## Submitting a pull request

1. Fork the repo and create a branch from `main` using the naming convention above.
2. Make focused commits following the commit message convention.
3. Ensure `make ci` passes locally.
4. Open a PR against `main` with a clear description of what and why.

We use **Rebase and merge** to keep a linear history on `main`.

## Reporting bugs

Open a [GitHub issue](https://github.com/kubeswarm/kubeswarm-cli/issues/new) with:

- swarm version (`swarm version` or `go version -m $(which swarm)`)
- The YAML file that triggered the issue (redact any secrets)
- Full terminal output with `--output json` if applicable

## Security vulnerabilities

See [SECURITY.md](./SECURITY.md) - please do **not** open a public issue.

## License

By contributing, you agree that your contributions will be licensed under the [Apache 2.0 License](./LICENSE).

# binix-action

A GitHub Action to cache Nix derivations with [Binix](https://github.com/krezh/binix).

## Usage

```yaml
- uses: krezh/binix-action@v1
  with:
    endpoint: https://binix.example.com
    cache: mycache
    token: ${{ secrets.BINIX_TOKEN }}
```

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `endpoint` | Binix server endpoint | Yes | - |
| `cache` | Binix cache name | Yes | - |
| `token` | Binix access token | No | - |
| `flake-ref` | Flake reference for binix installation | No | `github:krezh/binix` |
| `server-name` | Name for the server configuration | No | `default` |
| `skip-use` | Skip adding the cache as a substituter | No | `false` |
| `skip-push` | Skip pushing derivations to the cache | No | `false` |
| `jobs` | Number of parallel upload jobs | No | `5` |
| `chunk-concurrency` | Number of parallel chunk uploads | No | `4` |
| `include-paths` | Regex patterns for paths to include (one per line) | No | - |
| `exclude-paths` | Regex patterns for paths to exclude (one per line) | No | - |

## Example Workflow

```yaml
name: Build and Cache

on:
  push:
    branches: [main]
  pull_request:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: DeterminateSystems/nix-installer-action@main

      - uses: krezh/binix-action@v1
        with:
          endpoint: ${{ secrets.BINIX_ENDPOINT }}
          cache: ${{ secrets.BINIX_CACHE }}
          token: ${{ secrets.BINIX_TOKEN }}

      - run: nix build .#default
```

## How It Works

1. **Pre-build**: Installs binix, configures authentication, adds the cache as a substituter, and records existing store paths
2. **Build**: Your workflow runs `nix build` or other commands
3. **Post-build**: Compares store paths, pushes new derivations to the cache

## Development

```bash
cd binix-action
pnpm install
pnpm build
```

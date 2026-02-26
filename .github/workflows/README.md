# GitHub Actions Workflows

This directory contains CI/CD workflows for the Arni project.

## Workflows

### CI Workflow (`ci.yml`)

The main continuous integration workflow that runs on every push and pull request.

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches
- Manual dispatch via GitHub UI

**Jobs:**

#### 1. Format Check (`fmt`)
- Runs: Ubuntu latest
- Checks code formatting with `rustfmt`
- Fails if code is not properly formatted

#### 2. Clippy Lint (`clippy`)
- Runs: Ubuntu latest
- Runs Clippy linter with `-D warnings` (deny warnings)
- Checks all targets and features
- Uses caching for faster builds

#### 3. Test Matrix (`test`)
- Runs: Ubuntu, macOS, Windows
- Toolchains: stable, nightly
- Matrix: 6 combinations (3 OS × 2 toolchains)
- Steps:
  - Compilation check
  - Build project
  - Run unit tests
  - Run integration tests
- Uses caching for dependencies and build artifacts

#### 4. Build Release (`build-release`)
- Runs: Ubuntu latest
- Builds optimized release binary
- Uploads artifact for 7 days

#### 5. Minimal Versions Check (`minimal-versions`)
- Runs: Ubuntu latest with nightly
- Ensures code works with minimal dependency versions
- Helps maintain broad compatibility

#### 6. CI Success (`ci-success`)
- Summary job required for branch protection
- Succeeds only if all critical jobs pass
- Use this job for GitHub branch protection rules

## Local Testing

Test the workflow locally using [act](https://github.com/nektos/act):

```bash
# Install act (macOS)
brew install act

# Run all jobs
act

# Run specific job
act -j test

# Run with specific event
act pull_request
```

## Branch Protection

Configure branch protection rules in GitHub Settings:

1. Go to Settings → Branches → Branch protection rules
2. Add rule for `main` branch
3. Enable "Require status checks to pass before merging"
4. Select "CI Success" as required check
5. Enable "Require branches to be up to date before merging"

## Caching Strategy

The workflow uses GitHub Actions cache for:
- `~/.cargo/registry` - Cargo registry cache
- `~/.cargo/git` - Cargo git dependencies
- `target/` - Build artifacts

Caches are keyed by:
- OS
- Rust toolchain
- `Cargo.lock` hash

## Performance

Typical run times (as of 2026-02):
- **fmt**: ~30s
- **clippy**: ~2-3 minutes (with cache)
- **test** (per matrix): ~3-5 minutes (with cache)
- **build-release**: ~2-3 minutes (with cache)
- **Total**: ~10-15 minutes (all jobs complete)

First run without cache: ~20-30 minutes

## Environment Variables

```yaml
CARGO_TERM_COLOR: always  # Colored output
RUST_BACKTRACE: 1         # Backtraces on panic
```

## Artifact Retention

Release binaries are kept for 7 days. Download from the Actions run summary page.

## Future Enhancements

Planned additions:
- Coverage reporting (see arni-tks.9.2)
- Security scanning with cargo-deny (see arni-tks.9.3)
- Codecov integration
- Dependency update automation (Dependabot)
- Release automation
- Docker image builds

## Troubleshooting

### Workflow Not Triggering

- Check that branch names match triggers (`main`, `develop`)
- Verify workflow file is in `.github/workflows/`
- Check workflow file syntax with `yamllint`

### Cache Misses

- Caches expire after 7 days of no access
- `Cargo.lock` changes invalidate cache
- Different OS/toolchain combinations have separate caches

### Test Failures

- Check individual job logs for details
- Reproduce locally: `cargo test --verbose`
- For platform-specific issues, test in container or VM

### Timeout Issues

- Default timeout: 360 minutes (6 hours)
- Typical runs: 10-15 minutes
- If jobs timeout, check for hanging tests or infinite loops

## Related Documentation

- [Build Scripts](../../scripts/README.md)
- [Testing Guide](../../crates/arni/tests/README.md)
- [Contributing Guide](../../CONTRIBUTING.md)

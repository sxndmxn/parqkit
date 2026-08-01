# Releasing Parqkit

Releases are published from version tags by `.github/workflows/release.yml`.
The workflow verifies the complete release build, publishes the crate to
crates.io, and creates a GitHub Release with generated notes.

## One-Time Repository Setup

1. Create a crates.io API token authorized to publish `parqkit`.
2. Create a GitHub Actions environment named `release`.
3. Add the token to that environment as the `CARGO_REGISTRY_TOKEN` secret.
4. Optionally require approval for the `release` environment.

Keep the token only in the GitHub environment. Do not add it to the repository,
workflow file, shell history, or release artifacts.

## Release Procedure

1. Update the version in `Cargo.toml` and refresh `Cargo.lock`.
2. Add the dated release notes to `CHANGELOG.md`.
3. Merge the release commit into `master` and wait for CI to pass.
4. Create and push an annotated tag matching the package version:

   ```bash
   git tag -a v0.2.0 -m "parqkit 0.2.0"
   git push origin v0.2.0
   ```

The release workflow rejects a tag that does not exactly match the package
version or whose commit is not contained in `origin/master`. The crates.io
publish and GitHub Release are separate jobs, so a GitHub Release failure can
be retried without republishing the crate.

Published crate versions and pushed release tags are immutable release records.
If verification or publishing fails, fix the underlying problem and create a
new version rather than moving a tag that has already been pushed.

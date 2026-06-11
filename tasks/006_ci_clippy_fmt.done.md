# Enforce clippy and rustfmt in CI

ci.yml has no `cargo clippy --all-targets -- -D warnings` or
`cargo fmt --check` step. Add both (fix any existing violations first).

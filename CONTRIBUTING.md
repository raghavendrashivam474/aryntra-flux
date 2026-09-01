# Contributing to Aryntra Flux

## Development Standards
To ensure the project remains stable, all contributions must pass the following checks:

1. **Formatting**: All code must be formatted with \ustfmt\.
   \\\ash
   cargo fmt --check
   \\\

2. **Linting**: No Clippy warnings are allowed.
   \\\ash
   cargo clippy -- -D warnings
   \\\

3. **Testing**: All logic should be accompanied by unit or integration tests.
   \\\ash
   cargo test
   \\\

## Branching Logic
- **main**: Production-ready code.
- **sprint/S#.#**: Active development for specific sprints.

## Environment Diagnostics
Before submitting a PR, ensure \lux doctor\ reports a **READY** state.

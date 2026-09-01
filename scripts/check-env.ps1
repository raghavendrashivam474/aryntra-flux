Write-Host "Verifying Aryntra Flux Development Environment..." -ForegroundColor Cyan
cargo --version
cargo fmt --version
cargo clippy --version
cargo test
Write-Host "System ready for S1.2" -ForegroundColor Green

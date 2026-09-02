# Sprint S1.3 Demonstration Script
# Demonstrates two independent Flux nodes communicating over TCP transport

Write-Host "`n=======================================================" -ForegroundColor Cyan
Write-Host "   Aryntra Flux S1.3 - Transport Communication Demo    " -ForegroundColor Cyan
Write-Host "=======================================================`n" -ForegroundColor Cyan

# Step 1: Build binaries
Write-Host "[1/3] Building flux-node..." -ForegroundColor Yellow
cargo build --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "Build successful.`n" -ForegroundColor Green

# Step 2: Start Node B (Listener) in background
Write-Host "[2/3] Starting Node B (Listener on 0.0.0.0:9002)..." -ForegroundColor Yellow
$listenerJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    cargo run --quiet --bin flux-node -- --profile nodeB listen --port 9002
}
Start-Sleep -Seconds 2

# Step 3: Run Node A (Client) to connect to Node B
Write-Host "[3/3] Running Node A (Client) to connect to Node B..." -ForegroundColor Yellow
cargo run --quiet --bin flux-node -- --profile nodeA connect --addr 127.0.0.1:9002 --message "Hello from Node A via Flux TCP Transport!"

Write-Host "`n--- Node B (Listener) Output ---" -ForegroundColor DarkCyan
Receive-Job -Job $listenerJob

# Cleanup
Stop-Job -Job $listenerJob -ErrorAction SilentlyContinue | Out-Null
Remove-Job -Job $listenerJob -ErrorAction SilentlyContinue | Out-Null

Write-Host "`n=======================================================" -ForegroundColor Green
Write-Host "   S1.3 Demo Completed Successfully!                  " -ForegroundColor Green
Write-Host "=======================================================`n" -ForegroundColor Green

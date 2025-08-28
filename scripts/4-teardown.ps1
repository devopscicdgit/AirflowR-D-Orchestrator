# scripts/4-teardown.ps1

Write-Host "🛑 Tearing down Airflow environment..." -ForegroundColor Cyan

# Get project root (one level up from this script)
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# Navigate to project root to run docker-compose
Push-Location $ProjectRoot

# Stop and remove containers, networks, and volumes
Write-Host "🐳 Stopping Docker containers and removing volumes..."
docker compose down --volumes

# Return to original directory
Pop-Location

# Paths
$dataPath = Join-Path $ProjectRoot "data"
$datasetsPath = Join-Path $dataPath "datasets"
$logsPath = Join-Path $ProjectRoot "logs"
$envPath = Join-Path $ProjectRoot "env"

# Delete everything in 'data' except 'datasets'
Write-Host "🧹 Cleaning data directory but keeping datasets/..."
Get-ChildItem $dataPath -Recurse | Where-Object {
    $_.FullName -ne $datasetsPath -and 
    -not $_.FullName.StartsWith($datasetsPath)
} | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Ensure datasets folder still exists
if (-Not (Test-Path $datasetsPath)) {
    Write-Host "⚠️ datasets/ folder was missing — recreating it..."
    New-Item -ItemType Directory -Path $datasetsPath | Out-Null
}

# Remove logs
Write-Host "🗑️ Removing logs directory..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $logsPath

# Remove Python virtual environment
Write-Host "🗑️ Removing Python virtual environment if it exists..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $envPath

Write-Host "✅ Teardown complete (datasets preserved)!" -ForegroundColor Green

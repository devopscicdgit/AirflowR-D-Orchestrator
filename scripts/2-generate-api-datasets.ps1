# scripts/2-generate-api-datasets.ps1

Write-Host "🚀 Running dataset generation script..." -ForegroundColor Cyan

# Get project root (one level up from this script)
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# Path to virtual env activation (in root)
$EnvActivateScript = Join-Path $ProjectRoot "env\Scripts\Activate.ps1"

# Path to generate-experimental-datasets.py (in scripts folder, same as this script)
$DatasetScript = Join-Path $PSScriptRoot "generate-experimental-datasets.py"

# Check if virtual environment activation script exists
if (!(Test-Path $EnvActivateScript)) {
    Write-Error "❌ Virtual environment activation script not found at $EnvActivateScript. Please run 1-install.ps1 first."
    exit 1
}

# Check if generate-experimental-datasets.py exists
if (!(Test-Path $DatasetScript)) {
    Write-Error "❌ Dataset generation script not found at $DatasetScript."
    exit 1
}

# Activate virtual environment
Write-Host "🔄 Activating virtual environment..."
& $EnvActivateScript

# Run the Python dataset generation script
Write-Host "🐍 Running generate-experimental-datasets.py..."
python $DatasetScript

Write-Host "✅ Dataset generation complete!" -ForegroundColor Green

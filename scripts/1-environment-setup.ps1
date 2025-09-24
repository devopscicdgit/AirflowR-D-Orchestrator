# scripts/1-evironment-setup.ps1

Write-Host "🚀 Starting Python environment setup..." -ForegroundColor Cyan

# Get project root (one level up from this script)
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# Define paths
$EnvPath = Join-Path $ProjectRoot "env"
$ActivateScript = Join-Path $EnvPath "Scripts\Activate.ps1"
$RequirementsPath = Join-Path $ProjectRoot "requirements.txt"

# Check if Python 3.12 is available
$pythonVersion = & py -3.12 --version 2>$null
if (-not $pythonVersion) {
    Write-Error "❌ Python 3.12 is not installed or not available via 'py -3.12'. Please install it first."
    exit 1
}

# Create virtual environment if it doesn't exist
if (!(Test-Path $EnvPath)) {
    Write-Host "📦 Creating virtual environment at $EnvPath..."
    py -3.12 -m venv $EnvPath
} else {
    Write-Host "✅ Virtual environment already exists. Skipping creation."
}

# Activate the virtual environment
Write-Host "🔄 Activating virtual environment..."
& $ActivateScript

# Upgrade pip using python -m pip (recommended)
Write-Host "⬆️ Upgrading pip..."
python -m pip install --upgrade pip

# Install dependencies from requirements.txt
if (Test-Path $RequirementsPath) {
    Write-Host "📥 Installing dependencies from requirements.txt..."
    pip install -r $RequirementsPath
    Write-Host "✅ Dependencies installed successfully." -ForegroundColor Green
} else {
    Write-Error "❌ requirements.txt not found at $RequirementsPath"
    exit 1
}

# scripts/3-init-airflow.ps1

Write-Host "🚀 Initializing Airflow..." -ForegroundColor Cyan

# Get project root (assumes this script is inside 'scripts' folder)
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# Create required directories
Write-Host "📁 Creating data and logs directories..."
New-Item -ItemType Directory -Force -Path "$ProjectRoot\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$ProjectRoot\logs" | Out-Null

# Navigate to project root to run docker-compose
Push-Location $ProjectRoot

# Initialize the Airflow DB
Write-Host "🔧 Initializing the Airflow database..."
docker-compose run --rm webserver airflow db init

# Create admin user
Write-Host "👤 Creating admin user..."
docker-compose run --rm webserver airflow users create `
    --username admin `
    --firstname admin `
    --lastname admin `
    --role Admin `
    --password admin `
    --email admin@example.com

# Start the stack
Write-Host "🐳 Starting Docker services..."
docker-compose up --build -d

# Return to original directory
Pop-Location

Write-Host "✅ Airflow setup complete!" -ForegroundColor Green

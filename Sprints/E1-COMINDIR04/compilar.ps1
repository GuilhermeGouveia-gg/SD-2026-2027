param([string]$Javac = 'javac')
$ErrorActionPreference = 'Stop'
foreach ($role in @('produtor', 'lider', 'replica', 'processador')) {
    $project = Join-Path $PSScriptRoot "rep04-$role"
    $output = Join-Path $project 'out'
    New-Item -ItemType Directory -Force -Path $output | Out-Null
    $sources = @(Get-ChildItem -LiteralPath (Join-Path $project 'src/rep04') -Filter '*.java' | ForEach-Object FullName)
    & $Javac --release 11 -encoding UTF-8 -Xlint:all -d $output @sources
    if ($LASTEXITCODE -ne 0) { throw "A compilação de rep04-$role falhou." }
    Write-Host "OK: rep04-$role"
}

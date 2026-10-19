param([string]$Javac = 'javac')

$ErrorActionPreference = 'Stop'
foreach ($project in @('rep02-produtor', 'rep02-lider', 'rep02-replica')) {
    $projectPath = Join-Path $PSScriptRoot $project
    $sources = @(Get-ChildItem -LiteralPath (Join-Path $projectPath 'src/rep02') -Filter '*.java' |
        Select-Object -ExpandProperty FullName)
    # Cada projeto tem o seu classpath; as cópias das classes comuns não colidem.
    & $Javac --release 11 -encoding UTF-8 -Xlint:all -d (Join-Path $projectPath 'out') @sources
    if ($LASTEXITCODE -ne 0) { throw "Falha ao compilar $project" }
    Write-Output "Compilado: $project"
}

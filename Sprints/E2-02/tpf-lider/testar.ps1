param(
    [string]$Jdk = $env:JAVA_HOME,
    [switch]$SemAcl
)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    if ($Jdk) {
        $javac = Join-Path $Jdk 'bin\javac.exe'
        $java = Join-Path $Jdk 'bin\java.exe'
    } else {
        $javac = (Get-Command javac -ErrorAction Stop).Source
        $java = (Get-Command java -ErrorAction Stop).Source
    }
    New-Item -ItemType Directory -Path target\test-classes,target\test-data -Force | Out-Null
    # Testar o núcleo sem contactar Discord nem resolver dependências Maven.
    $sources = @(Get-ChildItem src\main\java -Recurse -Filter *.java |
        Where-Object Name -ne 'LeaderBot.java' | ForEach-Object FullName)
    $sources += @(Get-ChildItem src\test\java -Recurse -Filter *.java | ForEach-Object FullName)
    & $javac --release 17 -encoding UTF-8 -d target\test-classes @sources
    if ($LASTEXITCODE -ne 0) { throw 'A compilação dos testes falhou.' }
    $options = @("-Djava.io.tmpdir=$((Resolve-Path target\test-data).Path)")
    # Esta opção só existe no runner de testes; não altera o arranque do LeaderBot.
    if ($SemAcl) { $options += '-Dtpf.tests.semAcl=true' }
    & $java @options -cp target\test-classes tpf.ArquivoTests
    if ($LASTEXITCODE -ne 0) { throw 'Os testes falharam.' }
} finally { Pop-Location }

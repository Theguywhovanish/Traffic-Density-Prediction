[CmdletBinding()]
param(
    [string]$PythonPath
)

$ErrorActionPreference = 'Stop'

if ($env:OS -ne 'Windows_NT') {
    Write-Host 'Windows Hadoop helpers are only needed on Windows; no setup was performed.'
    return
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoHadoopHome = Join-Path $repoRoot '.tools\hadoop'

if ($env:HADOOP_HOME -and (Test-Path -LiteralPath (Join-Path $env:HADOOP_HOME 'bin\winutils.exe'))) {
    Write-Host "HADOOP_HOME already points to a Hadoop installation: $env:HADOOP_HOME"
    return
}

if (-not $PythonPath) {
    $venvPython = Join-Path $repoRoot '.venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $venvPython) {
        $PythonPath = $venvPython
    }
    else {
        $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
        if ($pythonCommand) {
            $PythonPath = $pythonCommand.Source
        }
    }
}

if (-not $PythonPath) {
    throw 'Python was not found. Create the project .venv, install requirements-spark.txt, then run this script again.'
}

if (-not (Test-Path -LiteralPath $PythonPath)) {
    $pythonCommand = Get-Command $PythonPath -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        $PythonPath = $pythonCommand.Source
    }
    else {
        throw "Python executable not found: $PythonPath"
    }
}

$pysparkPath = (& $PythonPath -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)').Trim()
if ($LASTEXITCODE -ne 0 -or -not $pysparkPath) {
    throw 'PySpark is not installed in the selected Python environment. Run `pip install -r requirements-spark.txt`, then rerun this script.'
}

$jarsPath = Join-Path $pysparkPath 'jars'
$hadoopJar = Get-ChildItem -LiteralPath $jarsPath -Filter 'hadoop-client-api-*.jar' -File -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $hadoopJar) {
    $hadoopJar = Get-ChildItem -LiteralPath $jarsPath -Filter 'hadoop-client-runtime-*.jar' -File -ErrorAction SilentlyContinue | Select-Object -First 1
}
if (-not $hadoopJar -or $hadoopJar.Name -notmatch '^hadoop-client-(?:api|runtime)-(?<version>\d+\.\d+\.\d+)\.jar$') {
    throw "Could not determine the Hadoop version bundled with PySpark under $jarsPath."
}
$runtimeVersion = $Matches.version

# PySpark 3.5 bundles Hadoop 3.3.4. The configured Windows source provides
# checksum-pinned 3.3.5 binaries, which are compatible with the 3.3.4 runtime.
switch ($runtimeVersion) {
    '3.3.4' { $helperVersion = '3.3.5' }
    '3.3.5' { $helperVersion = '3.3.5' }
    default {
        throw "This setup currently supports PySpark's Hadoop 3.3.4 or 3.3.5 runtime; found $runtimeVersion."
    }
}

$artifacts = @(
    @{
        Name   = 'winutils.exe'
        Sha256 = 'A0CA6E358357C41EF56EBDB02C38E4A4D55DA7CA7A13001678BB2EF7D644ADEA'
    },
    @{
        Name   = 'hadoop.dll'
        Sha256 = 'D3DD64AFDC85F2A7EB5345ABF2ECAA744B0A157DE40859313337D47F81EE1C7B'
    }
)

$targetBin = Join-Path $repoHadoopHome 'bin'
$installIsValid = $true
foreach ($artifact in $artifacts) {
    $targetFile = Join-Path $targetBin $artifact.Name
    if (-not (Test-Path -LiteralPath $targetFile)) {
        $installIsValid = $false
        break
    }
    $actualHash = (Get-FileHash -LiteralPath $targetFile -Algorithm SHA256).Hash
    if (-not [string]::Equals($actualHash, $artifact.Sha256, [StringComparison]::OrdinalIgnoreCase)) {
        $installIsValid = $false
        break
    }
}

if ($installIsValid) {
    Write-Host "Verified existing repo-local Hadoop helpers for runtime $runtimeVersion."
}
else {
    $stageRoot = Join-Path ([IO.Path]::GetTempPath()) ('urban-mobility-hadoop-' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $stageRoot -Force | Out-Null
    try {
        foreach ($artifact in $artifacts) {
            $uri = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-$helperVersion/bin/$($artifact.Name)"
            $stageFile = Join-Path $stageRoot $artifact.Name
            Write-Host "Downloading $($artifact.Name) from cdarlint/winutils..."
            Invoke-WebRequest -Uri $uri -OutFile $stageFile

            $actualHash = (Get-FileHash -LiteralPath $stageFile -Algorithm SHA256).Hash
            if (-not [string]::Equals($actualHash, $artifact.Sha256, [StringComparison]::OrdinalIgnoreCase)) {
                throw "SHA-256 verification failed for $($artifact.Name). The file was not installed."
            }
        }

        New-Item -ItemType Directory -Path $targetBin -Force | Out-Null
        foreach ($artifact in $artifacts) {
            Copy-Item -LiteralPath (Join-Path $stageRoot $artifact.Name) -Destination (Join-Path $targetBin $artifact.Name) -Force
        }

        foreach ($artifact in $artifacts) {
            $installedFile = Join-Path $targetBin $artifact.Name
            $installedHash = (Get-FileHash -LiteralPath $installedFile -Algorithm SHA256).Hash
            if (-not [string]::Equals($installedHash, $artifact.Sha256, [StringComparison]::OrdinalIgnoreCase)) {
                throw "Installed helper verification failed for $($artifact.Name)."
            }
        }
    }
    finally {
        $tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
        $stageFullPath = [IO.Path]::GetFullPath($stageRoot)
        if ($stageFullPath.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) {
            Remove-Item -LiteralPath $stageFullPath -Recurse -Force -ErrorAction SilentlyContinue
        }
    }

    Write-Host "Installed checksum-verified Hadoop $helperVersion Windows helpers for PySpark's Hadoop $runtimeVersion runtime."
}

$env:HADOOP_HOME = $repoHadoopHome
$env:PATH = (Join-Path $repoHadoopHome 'bin') + [IO.Path]::PathSeparator + $env:PATH
Write-Host "HADOOP_HOME is set to $repoHadoopHome for this PowerShell process."
Write-Host 'The Python Spark session also discovers this repo-local setup automatically.'

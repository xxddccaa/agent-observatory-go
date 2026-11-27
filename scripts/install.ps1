[CmdletBinding()]
param(
  [string]$Version = $env:AGENT_OBS_VERSION,
  [string]$InstallDir = $(if ($env:AGENT_OBS_INSTALL_DIR) { $env:AGENT_OBS_INSTALL_DIR } else { Join-Path $env:LocalAppData "agent-observatory\bin" }),
  [string]$Repo = $(if ($env:AGENT_OBS_REPO) { $env:AGENT_OBS_REPO } else { "xxddccaa/agent-observatory-go" }),
  [string]$BaseUrl = $env:AGENT_OBS_BASE_URL,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$AppName = "agent-observatory"

try {
  # Enable TLS 1.2 for Windows PowerShell 5.1 before calling GitHub.
  [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
} catch {
}

function Resolve-Arch {
  $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
  switch ($arch) {
    "x64" { return "amd64" }
    default { throw "Unsupported Windows architecture: $arch (only amd64 is built in V1)" }
  }
}

function Resolve-LatestVersion {
  param([string]$RepoSlug)
  $LatestUrl = "https://github.com/$RepoSlug/releases/latest"
  try {
    # Prefer GitHub's web redirect over api.github.com to avoid the 60 req/hr unauthenticated IP rate limit.
    $Req = [System.Net.HttpWebRequest]::Create($LatestUrl)
    $Req.Method = "HEAD"
    $Req.AllowAutoRedirect = $true
    $Resp = $Req.GetResponse()
    try {
      $FinalUrl = $Resp.ResponseUri.AbsoluteUri
    } finally {
      $Resp.Close()
    }
    if ($FinalUrl -match "/releases/tag/([^/?#]+)") {
      return $Matches[1]
    }
  } catch {
    # Fall back to GitHub REST API below.
  }

  $LatestApi = "https://api.github.com/repos/$RepoSlug/releases/latest"
  $Release = Invoke-RestMethod -Uri $LatestApi -Headers @{ "Accept" = "application/vnd.github+json" }
  $Tag = $Release.tag_name
  if ([string]::IsNullOrWhiteSpace($Tag)) {
    throw "Failed to resolve latest release tag from $LatestUrl or $LatestApi"
  }
  return $Tag
}

$Arch = Resolve-Arch

if ([string]::IsNullOrWhiteSpace($Version)) {
  if (-not [string]::IsNullOrWhiteSpace($BaseUrl)) {
    throw "-Version / AGENT_OBS_VERSION is required when BaseUrl is overridden."
  }
  $Version = Resolve-LatestVersion -RepoSlug $Repo
}

$VersionNum = $Version.TrimStart("v")
$Archive = "${AppName}_${VersionNum}_windows_${Arch}.zip"

if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
  $BaseUrl = "https://github.com/$Repo/releases/download/v$VersionNum"
}
$BaseUrl = $BaseUrl.TrimEnd("/")
$ArchiveUrl = "$BaseUrl/$Archive"
$ChecksumsUrl = "$BaseUrl/checksums.txt"

if ($DryRun) {
  Write-Output "dry-run: os=windows arch=$Arch version=v$VersionNum"
  Write-Output "dry-run: archive=$ArchiveUrl"
  Write-Output "dry-run: checksums=$ChecksumsUrl"
  Write-Output "dry-run: install_path=$(Join-Path $InstallDir "$AppName.exe")"
  exit 0
}

$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("agobs-install-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

try {
  $ArchivePath = Join-Path $TempDir $Archive
  $ChecksumsPath = Join-Path $TempDir "checksums.txt"
  $ExtractDir = Join-Path $TempDir "extract"

  Write-Host "Downloading $ArchiveUrl..."
  Invoke-WebRequest -UseBasicParsing -Uri $ArchiveUrl -OutFile $ArchivePath
  Invoke-WebRequest -UseBasicParsing -Uri $ChecksumsUrl -OutFile $ChecksumsPath

  $ExpectedLine = Get-Content -Path $ChecksumsPath | Where-Object { $_ -match ("^\s*([0-9a-fA-F]{64})\s+" + [Regex]::Escape($Archive) + "\s*$") } | Select-Object -First 1
  if (-not $ExpectedLine) {
    throw "Checksum entry missing for $Archive in checksums.txt"
  }
  $ExpectedHash = ($ExpectedLine -split "\s+")[0].ToLowerInvariant()
  $ActualHash = (Get-FileHash -Path $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($ActualHash -ne $ExpectedHash) {
    throw "SHA-256 verification failed for ${Archive}: expected $ExpectedHash, got $ActualHash"
  }

  Expand-Archive -Path $ArchivePath -DestinationPath $ExtractDir -Force
  $ExtractedExe = Join-Path $ExtractDir "$AppName.exe"
  if (-not (Test-Path $ExtractedExe)) {
    throw "Archive did not contain expected binary $AppName.exe"
  }

  New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
  $DestExe = Join-Path $InstallDir "$AppName.exe"
  $WasRunning = $false
  if (Test-Path $DestExe) {
    & $DestExe status *> $null
    if ($LASTEXITCODE -eq 0) {
      $WasRunning = $true
      # Stop the running binary first so Windows releases the file lock before Move-Item.
      & $DestExe stop *> $null
    }
  }

  $StageExe = Join-Path $InstallDir ".$AppName.exe.tmp"
  Copy-Item -Path $ExtractedExe -Destination $StageExe -Force
  Move-Item -Path $StageExe -Destination $DestExe -Force
  $AliasCmd = Join-Path $InstallDir "aobs.cmd"
  Set-Content -Path $AliasCmd -Value "@echo off`r`n`"%~dp0$AppName.exe`" %*`r`n" -Encoding Ascii

  $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
  $PathEntries = if ($UserPath) { $UserPath -split ";" } else { @() }
  if (-not ($PathEntries | Where-Object { $_.TrimEnd("\") -ieq $InstallDir.TrimEnd("\") })) {
    $NewUserPath = if ($UserPath) { "$UserPath;$InstallDir" } else { $InstallDir }
    [Environment]::SetEnvironmentVariable("Path", $NewUserPath, "User")
    Write-Host "Added $InstallDir to your User PATH (restart terminal to take effect)."
  }

  $VerOut = & $DestExe --version
  Write-Host "Installed $VerOut to $DestExe"
  if ($WasRunning) {
    & $DestExe --no-open *> $null
    if ($LASTEXITCODE -eq 0) {
      Write-Host "Restarted running $AppName daemon in background."
    } else {
      Write-Warning "Stopped previous daemon; run 'aobs' to start the updated version."
    }
  }
}
finally {
  Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
}

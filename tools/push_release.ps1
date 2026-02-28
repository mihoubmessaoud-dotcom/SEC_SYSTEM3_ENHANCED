param(
    [Parameter(Mandatory = $true)]
    [string]$RemoteUrl,
    [string]$Branch = "master",
    [string]$Tag = "v4-definitive"
)

$ErrorActionPreference = "Stop"

$repo = (Get-Location).Path

Write-Host "Repository: $repo"
Write-Host "Target remote: $RemoteUrl"
Write-Host "Branch: $Branch"
Write-Host "Tag: $Tag"

if (-not (Test-Path ".git")) {
    throw "No .git directory found in current path."
}

$hasOrigin = (git remote) -contains "origin"
if ($hasOrigin) {
    git remote set-url origin $RemoteUrl
} else {
    git remote add origin $RemoteUrl
}

git fetch origin
git push -u origin $Branch
git push origin $Tag

Write-Host "Push complete."

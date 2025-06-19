param(
    [int]$firstChunk = 6500,
    [int]$lastChunk  = 26420,
    [int]$step       = 500
)

$repo  = "vGassen/Dutch-Basisbestandwetten-Legislation-Laws"
$token = $env:HF_TOKEN
if (-not $token) { throw "❌ يجب ضبط HF_TOKEN" }

for ($i = $firstChunk; $i -lt $lastChunk; $i += $step) {
    $s = $i
    $e = [math]::Min($i + $step, $lastChunk)
    Write-Host "📤 رفع الشريحة $s → $e ..." -ForegroundColor Cyan

    $args = @(
        ""shard_upload_resume.py",
",
        "--repo_id",  $repo,
        "--token",    $token,
        "--start",    $s,
        "--end",      $e,
        "--shard_size", $step
    )
    python $args
    if ($LASTEXITCODE -ne 0) {
        Write-Host "🛑 توقف عند الشريحة $s → $e." -ForegroundColor Red
        break
    }
    Write-Host "✅ تم رفع الشريحة $s → $e" -ForegroundColor Green
    Start-Sleep -Seconds 5
}
Write-Host "🎉 انتهى السكربت." -ForegroundColor Yellow

$start =6500
$end = 26400
$step = 500
$token = $env:HF_TOKEN
$repo = "Moha8med80/Access_model"

for ($i = $start; $i -lt $end; $i += $step) {
    $s = $i
    $e = $i + $step
    Write-Host "📤 رفع من $s إلى $e ..."
    python .\shard_upload_dataset.py --repo_id $repo --token $token --start $s --end $e --shard_size $step
    Start-Sleep -Seconds 5
}

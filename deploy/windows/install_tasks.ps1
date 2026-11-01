# GoldXFlow on the Windows trading PC: scheduled tasks that start at logon and restart on failure.
# Run in an elevated PowerShell from the project folder (e.g. C:\Users\you\Documents\python\goldflow):
#   powershell -ExecutionPolicy Bypass -File deploy\windows\install_tasks.ps1
# Secrets: set them as user environment variables (setx GOLDFLOW_BRIDGE_SECRET ...), or use the bridge.secret file in
# MT5 Common Files, never in the config. MT5 itself must be running with GoldflowExecutor attached (demo account first).
param([string]$Root = (Get-Location).Path, [string]$Python = "$((Get-Location).Path)\.venv\Scripts\python.exe")

$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable
$logon = New-ScheduledTaskTrigger -AtLogOn

function Add-GoldflowTask($name, $cliArgs, $trigger) {
  $action = New-ScheduledTaskAction -Execute $Python -Argument "-m goldflow.cli $cliArgs" -WorkingDirectory $Root
  Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
  Write-Host "registered $name"
}

# Demo routing first (profile demo). Switch to the live profile only after `exec-gate` has recorded the M7 gate.
Add-GoldflowTask "GoldXFlow-Bridge"    "bridge-run --profile demo --name demo1" $logon
Add-GoldflowTask "GoldXFlow-Dashboard" "dashboard" $logon
$every = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 1)
Add-GoldflowTask "GoldXFlow-OpsCheck"  "ops-check" $every
$daily = New-ScheduledTaskTrigger -Daily -At 3:17am
Add-GoldflowTask "GoldXFlow-Backup"    "backup --dest D:\goldflow-backup" $daily
Write-Host "Done. Dashboard: http://127.0.0.1:8765/  Emergency stop: python -m goldflow.cli emergency-stop --name demo1"

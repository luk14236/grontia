# Registers a Windows scheduled task that starts the WSL distro at logon and
# keeps it alive. Docker runs under systemd inside WSL and restarts the Grontia
# stack on its own (restart: unless-stopped), so Airflow is up without a terminal.
#
# Run in Windows PowerShell (as administrator if it asks):
#   powershell -ExecutionPolicy Bypass -File scripts\local\register-wsl-autostart.ps1
# Remove with:
#   Unregister-ScheduledTask -TaskName "Grontia WSL autostart"

param([string]$Distro = "Ubuntu")

# conhost --headless keeps the console window from popping up at logon;
# "sleep infinity" keeps the WSL VM from shutting down when idle.
$action = New-ScheduledTaskAction -Execute "conhost.exe" `
    -Argument "--headless wsl.exe -d $Distro --exec sleep infinity"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask -TaskName "Grontia WSL autostart" -Action $action -Trigger $trigger `
    -Settings $settings -Force `
    -Description "Starts WSL ($Distro) at logon so Docker and the Grontia Airflow stack run."

Write-Host "Registered. WSL ($Distro) will start at your next logon."

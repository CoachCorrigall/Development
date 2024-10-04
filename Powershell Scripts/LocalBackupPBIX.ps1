	# Set your Power BI workspace ID and the folder where you want to store the PBIX backups
	$workspaceId = "Power BI Service Workspace ID"
	$backupFolder = "C:\PBIXBackups\20241004\<Workspace Name>"
	
	# Ensure the backup folder exists
	if (!(Test-Path -Path $backupFolder)) {
	    New-Item -ItemType Directory -Path $backupFolder
	}
	
	# Get the list of reports in the workspace
	$reports = Get-PowerBIReport -WorkspaceId $workspaceId
	
	# Loop through each report and export the PBIX file
	foreach ($report in $reports) {
	    $pbixFilePath = "$backupFolder\$($report.Name).pbix"
	
	    # Export the report
	    Export-PowerBIReport -WorkspaceId $workspaceId -ReportId $report.Id -OutFile $pbixFilePath
	
	    Write-Host "Backup created for report: $($report.Name) at $pbixFilePath"
}
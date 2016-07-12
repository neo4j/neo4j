$DebugPreference = "SilentlyContinue"

$here = Split-Path -Parent $MyInvocation.MyCommand.Definition
$src = Resolve-Path -Path "$($here)\..\..\main\distribution\shell-scripts\bin\Neo4j-Management"

# Helper functions must be created in the global scope due to the InModuleScope command

Function global:New-MockNeo4jInstall($RootDir, $IncludeFiles = $true, $ServerType = 'Community', $ServerVersion = '0.0') {
  # Creates a skeleton directory and file structure of a Neo4j Installation
  New-Item $RootDir -ItemType Directory | Out-Null
  New-Item "$RootDir\system" -ItemType Directory | Out-Null
  New-Item "$RootDir\system\lib" -ItemType Directory | Out-Null
  
  if ($IncludeFiles) {
    'TempFile' | Out-File -FilePath "$RootDir\system\lib\neo4j-server-$($ServerVersion).jar"
    if ($ServerType -eq 'Enterprise') { 'TempFile' | Out-File -FilePath "$RootDir\system\lib\neo4j-server-enterprise-$($ServerVersion).jar" }
    if ($ServerType -eq 'Advanced') { 'TempFile' | Out-File -FilePath "$RootDir\system\lib\neo4j-server-advanced-$($ServerVersion).jar" }
  }  
}
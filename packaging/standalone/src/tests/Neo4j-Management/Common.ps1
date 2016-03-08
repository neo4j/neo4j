$DebugPreference = "SilentlyContinue"

$here = Split-Path -Parent $MyInvocation.MyCommand.Definition
$src = Resolve-Path -Path "$($here)\..\..\main\distribution\shell-scripts\bin\Neo4j-Management"

# Helper functions must be created in the global scope due to the InModuleScope command

Function global:New-MockNeo4jInstall($RootDir, $IncludeFiles = $true, $ServerType = 'Community', $ServerVersion = '0.0', $DatabaseMode = '') {
  # Creates a skeleton directory and file structure of a Neo4j Installation
  New-Item $RootDir -ItemType Directory | Out-Null
  New-Item "$RootDir\lib" -ItemType Directory | Out-Null
  
  if ($IncludeFiles) {
    'TempFile' | Out-File -FilePath "$RootDir\lib\neo4j-server-$($ServerVersion).jar"
    if ($ServerType -eq 'Enterprise') { 'TempFile' | Out-File -FilePath "$RootDir\lib\neo4j-server-enterprise-$($ServerVersion).jar" }
  
    # Create fake neo4j.conf
    $neoConf = ''
  
    if ($DatabaseMode -ne '') {
      $neoConf += "dbms.mode=$DatabaseMode`n`r"
    }
    
  New-Item "$RootDir\conf" -ItemType Directory | Out-Null
    $neoConf | Out-File -FilePath "$RootDir\conf\neo4j.conf"
  }
}

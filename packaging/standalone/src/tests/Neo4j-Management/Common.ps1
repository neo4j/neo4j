$DebugPreference = "SilentlyContinue"

$here = Split-Path -Parent $MyInvocation.MyCommand.Definition
$src = Resolve-Path -Path "$($here)\..\..\main\distribution\shell-scripts\bin\Neo4j-Management"

# Helper functions must be created in the global scope due to the InModuleScope command
$global:mockServiceName = 'neo4j'
$global:mockNeo4jHome = 'TestDrive:\Neo4j'

Function global:New-MockJavaHome() {
  $javaHome = "TestDrive:\JavaHome"
  
  New-Item $javaHome -ItemType Directory | Out-Null
  New-Item "$javaHome\bin" -ItemType Directory | Out-Null
  New-Item "$javaHome\bin\server" -ItemType Directory | Out-Null
  
  "This is a mock java.exe" | Out-File "$javaHome\bin\java.exe"
  "This is a mock java.exe" | Out-File "$javaHome\bin\server\jvm.dll"

  return $javaHome
}

Function global:New-InvalidNeo4jInstall($ServerType = 'Enterprise', $ServerVersion = '99.99', $DatabaseMode = '') {
  $serverObject = (New-Object -TypeName PSCustomObject -Property @{
    'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
    'ConfDir' = 'TestDrive:\some-dir-that-doesnt-exist\conf';
    'ServerVersion' = $ServerVersion;
    'ServerType' = $ServerType;
    'DatabaseMode' = $DatabaseMode;
  })
  return $serverObject
}

Function global:New-MockNeo4jInstall($IncludeFiles = $true, $ServerType = 'Community', $ServerVersion = '0.0', $DatabaseMode = '', $WindowsService = $global:mockServiceName, $Lib = '') {
  # Creates a skeleton directory and file structure of a Neo4j Installation
  $customLib = ($Lib -ne '')
  if ($Lib -eq '') { $Lib = 'lib' }
  $RootDir = $global:mockNeo4jHome
  $LibDir = "$($RootDir)\$($Lib)"

  New-Item $RootDir -ItemType Directory | Out-Null
  New-Item $LibDir -ItemType Directory | Out-Null
  New-Item "$RootDir\bin" -ItemType Directory | Out-Null
  New-Item "$RootDir\bin\tools" -ItemType Directory | Out-Null
  New-Item "$RootDir\conf" -ItemType Directory | Out-Null
  
  if ($IncludeFiles) {
    'TempFile' | Out-File -FilePath "$LibDir\neo4j-server-$($ServerVersion).jar"
    if ($ServerType -eq 'Enterprise') { 'TempFile' | Out-File -FilePath "$LibDir\neo4j-server-enterprise-$($ServerVersion).jar" }

    # Additional Jars
    'TempFile' | Out-File -FilePath "$LibDir\lib1.jar"
    'TempFile' | Out-File -FilePath "$RootDir\bin\bin1.jar"
    
    # Procrun service files
    'TempFile' | Out-File -FilePath "$RootDir\bin\tools\prunsrv-amd64.exe"
    'TempFile' | Out-File -FilePath "$RootDir\bin\tools\prunsrv-i386.exe"
  
    # Create fake neo4j.conf
    $neoConf = ''
    if ($DatabaseMode -ne '') {
      $neoConf += "dbms.mode=$DatabaseMode`n`r"
    }    
    if ($customLib) {
      $neoConf += "dbms.directories.lib=$Lib`n`r"
    }    
    $neoConf | Out-File -FilePath "$RootDir\conf\neo4j.conf"

    # Create fake neo4j-wrapper.conf
    $neoConf = ''
    if ([string]$WindowsService -ne '') { $neoConf += "dbms.windows_service_name=$WindowsService`n`r" }
    $neoConf | Out-File -FilePath "$RootDir\conf\neo4j-wrapper.conf"
  }
  
  $serverObject = (New-Object -TypeName PSCustomObject -Property @{
    'Home' = $RootDir;
    'ConfDir' = "$RootDir\conf";
    'ServerVersion' = $ServerVersion;
    'ServerType' = $ServerType;
    'DatabaseMode' = $DatabaseMode;
    'LibDir' = $Lib;
  })
  return $serverObject
}

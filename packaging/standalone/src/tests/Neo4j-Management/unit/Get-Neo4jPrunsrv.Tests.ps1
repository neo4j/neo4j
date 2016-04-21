$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Get-Neo4jPrunsrv" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }
    # Mock Neo4j environment
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' } 

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall
 
      It "return throw if invalid or missing neo4j directory" {
        { Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall  -ErrorAction Stop }  | Should Throw      
      }
    }

    Context "Invalid or missing servicename in specified neo4j installation" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService ''
 
      It "return throw if invalid or missing service name" {
        { Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall  -ErrorAction Stop }  | Should Throw      
      }
    }

    Context "Select PRUNSRV based on OS architecture" {
      $serverObject = global:New-MockNeo4jInstall
      $testCases = @(
        @{ 'AddressWidth' = 32; 'exe' = 'prunsrv-i386.exe'},
        @{ 'AddressWidth' = 64; 'exe' = 'prunsrv-amd64.exe'}
      ) | ForEach-Object -Process {
        $testCase = $_
          Mock Get-WMIObject { @{ 'AddressWidth' = $testCase.AddressWidth}}
    
          $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

          It "return $($testCase.exe) on $($testCase.AddressWidth)bit operating system" {
            $prunsrv.cmd  | Should Match ([regex]::Escape($testCase.exe) + '$')
          }
        }
    }

    Context "PRUNSRV arguments" {
      $mockLib = 'mock_lib'
      $serverObject = global:New-MockNeo4jInstall -Lib $mockLib

      It "return //IS/xxx argument on service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//IS//$($global:mockServiceName)"))
      }

      It "return //DS/xxx argument on service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerUninstall

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//DS//$($global:mockServiceName)"))
      }

      It "return //TS/xxx argument on service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForConsole

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//TS//$($global:mockServiceName)"))
      }

      It "return configured lib ClassPath for service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("--ClassPath=$($mockLib)/*"))
      }
    }

    Context "Server Invoke - Community v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Community'

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

      It "should have main class of org.neo4j.server.CommunityEntryPoint" {
        ($prunsrv.args -join ' ') | Should Match ([regex]::Escape('=org.neo4j.server.CommunityEntryPoint'))
      }
    }

    Context "Server Invoke - Enterprise v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Enterprise'

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

      It "should have main class of org.neo4j.server.enterprise.EnterpriseEntryPoint" {
        ($prunsrv.args -join ' ') | Should Match ([regex]::Escape('=org.neo4j.server.enterprise.EnterpriseEntryPoint'))
      }
    }

    Context "Server Invoke - Enterprise Arbiter v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Enterprise' -DatabaseMode 'Arbiter'

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

      It "should have main class of org.neo4j.server.enterprise.ArbiterEntryPoint" {
        ($prunsrv.args -join ' ') | Should Match ([regex]::Escape('=org.neo4j.server.enterprise.ArbiterEntryPoint'))
      }
    }
  }
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Uninstall-Neo4jServer" {

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
    $serviceName = 'neo4j'
    Mock Get-Neo4jWindowsServiceName { return $serviceName }
    Mock Get-Service { @{ 'State' = 'Running' } } -ParameterFilter { $Name = $serviceName }
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Stop-Service { $true } -ParameterFilter { $Name -eq $serviceName}

    Context "Missing service name in configuration files" {
      Mock Get-Neo4jWindowsServiceName { throw "Invalid service name" }

      $serverObject = global:New-InvalidNeo4jInstall

      It "throws error for missing service name in configuration file" {
        { Uninstall-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Windows service does not exist" {
      Mock Get-Service -Verifiable { $null }

      $serverObject = global:New-MockNeo4jInstall
      
      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Uninstall windows service successfully" {
      Mock Start-Process { @{ 'ExitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
    }
    
    Context "During uninstall, does not stop service if already stopped" {
      Mock Get-Service { @{ 'State' = 'Stopped' } } -ParameterFilter { $Name = $serviceName }
      Mock Start-Process { @{ 'ExitCode' = 0 } }   
      Mock Stop-Service  { } 

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
      
      It "does not call Stop-Service" {
        Assert-MockCalled Stop-Service -Times 0
      }
    }
  }
}
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Start-Neo4jServer" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' }
    Mock Set-Neo4jEnv { }
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }
    # Mock Neo4j environment
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Start-Process { throw "Should not call Start-Process mock" }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "throws error for an invalid server object - Server" {
        { Start-Neo4jServer -Server -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }

      It "throws error for an invalid server object - Console" {
        { Start-Neo4jServer -Console -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    # Windows Service Tests
    Context "Missing service name in configuration files" {
      Mock Start-Service { }

      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "throws error for missing service name in configuration file" {
        { Start-Neo4jServer -Service -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Start service succesfully but not running" {
      Mock Start-Service { throw "Wrong Service name" }
      Mock Start-Service -Verifiable { @{ Status = 'Start Pending'} } -ParameterFilter { $Name -eq $global:mockServiceName }

      $serverObject = global:New-MockNeo4jInstall

      $result = Start-Neo4jServer -Service -Neo4jServer $serverObject

      It "result is 2" {
        $result | Should Be 2
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Start service succesfully" {
      Mock Start-Service { throw "Wrong Service name" }
      Mock Start-Service -Verifiable { @{ Status = 'Running'} } -ParameterFilter { $Name -eq $global:mockServiceName }

      $serverObject = global:New-MockNeo4jInstall

      $result = Start-Neo4jServer -Service -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    # Console Tests
    Context "Start as a process and missing Java" {
      Mock Get-Java { }
      Mock Start-Process { }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })
      It "throws error if missing Java" {
        { Start-Neo4jServer -Console -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }
  }
}
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jStatus" {

    $mockServerObject = global:New-MockNeo4jInstall
    Mock Set-Neo4jEnv { }
    Mock Get-Neo4jEnv { $mockServerObject.Home } -ParameterFilter { $Name -eq 'NEO4J_HOME' }

    Context "Missing service name in configuration files" {
      Mock -Verifiable Get-Neo4jWindowsServiceName { throw "Missing service name" }

      It "throws error for missing service name in configuration file" {
        { Get-Neo4jStatus -Neo4jServer $mockServerObject -ErrorAction Stop } | Should Throw
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Service not installed" {
      Mock Get-Service -Verifiable { throw "Missing Service"}

      $result = Get-Neo4jStatus -Neo4jServer $mockServerObject
      It "result is 3" {
        $result | Should Be 3
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Service not installed but not running" {
      Mock Get-Service -Verifiable { @{ Status = 'Stopped' }}

      $result = Get-Neo4jStatus -Neo4jServer $mockServerObject
      It "result is 3" {
        $result | Should Be 3
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Service is running" {
      Mock Get-Service -Verifiable { @{ Status = 'Running' }}

      $result = Get-Neo4jStatus -Neo4jServer $mockServerObject
      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
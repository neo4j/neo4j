$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Confirm-Neo4jHome" {
    Context "Invalid Neo4jHome path" {
      It "return false for missing parameter" {
        Confirm-Neo4jHome | Should Be $false
      }
      It "return false for a missing directory" {
        Confirm-Neo4jHome -Neo4jHome 'TestDrive:\Some-dir-that-doesnt-exist' | Should Be $false
      }
    }
  
    Context "Valid Neo4jHome path" {
      $neo4jPath = 'TestDrive:\SomePath'
      Mock Test-Path { $true } -ParameterFilter { $Path -eq $neo4jPath }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq "$($neo4jPath)\system\lib" }
  
      It "return true" {
        Confirm-Neo4jHome -Neo4jHome $neo4jPath | Should Be $true
      }
      It "return true for alias Home" {
        Confirm-Neo4jHome -Home $neo4jPath | Should Be $true
      }
      It "return true for piped input" {
        { $neo4jPath | Confirm-Neo4jHome } | Should Be $true
      }
    }
  }
}

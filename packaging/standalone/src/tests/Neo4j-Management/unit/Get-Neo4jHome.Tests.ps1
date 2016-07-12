$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jHome" {
    Context "Missing NEO4J_HOME environment variable" {
      # Setup
      [Environment]::SetEnvironmentVariable("NEO4J_HOME", "", "Process")
      Mock Test-Path { return $false }
      # Call it
      $neoHome = Get-Neo4jHome
      
      It "returns null" {
        $neoHome | Should BeNullOrEmpty
      }
      It "doesn not attempt to verify the path" {
        Assert-MockCalled Test-Path -Times 0
      }
    }
  
    Context "Invalid NEO4J_HOME environment variable" {
      # Setup      
      [Environment]::SetEnvironmentVariable("NEO4J_HOME", "TestDrive:\some-dir-that-doesnt-exist", "Process")
      Mock Test-Path { return $false }
      # Call it
      $neoHome = Get-Neo4jHome
      
      It "attempts to verify the path" {
        Assert-MockCalled Test-Path -Times 1
      }
      It "returns null" {
        $neoHome | Should BeNullOrEmpty
      }
    }
  
    Context "Valid NEO4J_HOME environment variable" {
      # Setup
      [Environment]::SetEnvironmentVariable("NEO4J_HOME", "TestDrive:\some-dir-that-doesnt-exist", "Process")
      Mock Test-Path { return $true }
      # Call it
      $neoHome = Get-Neo4jHome
      
      It "attempts to verify the path" {
        Assert-MockCalled Test-Path -Times 1
      }
      It "returns NEO4J_HOME" {
        $neoHome | Should Be "TestDrive:\some-dir-that-doesnt-exist"
      }
    }
  }
}
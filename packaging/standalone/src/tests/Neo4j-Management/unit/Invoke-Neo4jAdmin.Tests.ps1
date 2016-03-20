$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Invoke-Neo4jAdmin" {

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
    $mockNeo4jHome = global:New-MockNeo4jInstall
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' } 
    Mock Start-Process { throw "Should not call Start-Process mock" }
    # Mock helper functions
    Mock Invoke-Neo4jAdmin_Import { 2 }

    Context "No arguments" {
      $result = Invoke-Neo4jAdmin
      
      It "returns 1 if no arguments" {
        $result | Should Be 0
      }
    }

    # Helper functions - error
    Context "Helper function throws an error" {
      Mock Invoke-Neo4jAdmin_Import { throw "error" }

      It "returns non zero exit code on error" {
        Invoke-Neo4jAdmin 'import' -ErrorAction SilentlyContinue | Should Be 1
      }

      It "throws error when terminating error" {
        { Invoke-Neo4jAdmin 'import' -ErrorAction Stop } | Should Throw
      }
    }


    # Helper functions
    Context "Helper functions" {
      It "returns exitcode from import command" {
        Invoke-Neo4jAdmin 'import' | Should Be 2
      }
    }
  }
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Invoke-Neo4jAdmin" {

    Context "Nodes and Relationships as comma delimited list" {
      # Commands from the command line come through as System.Object[]
      # These commands can be simulated through crafting an appropriate array

      # neo4j-admin import --mode=database --fake "file1,file2"
      $testCommand = @('import','--setting=value','--fake', @('file1','file2'))

      Mock Invoke-Neo4jUtility { Write-Host $CommandArgs -ForegroundColor Magenta   ;return 2 }
      Mock Invoke-Neo4jUtility -Verifiable { return 0} -ParameterFilter {
        $Command -eq 'admintool' `
        -and $CommandArgs[0] -eq 'import' `
        -and $CommandArgs[1] -eq '--setting=value' `
        -and $CommandArgs[2] -eq '--fake' `
        -and $CommandArgs[3] -eq 'file1,file2' `
      }

      $result = Invoke-Neo4jAdmin -CommandArgs $testCommand
      It "Should return exit code 0" {
        $result | Should Be 0
      }

      It "Should call verified mocks" {
        Assert-VerifiableMocks
      }
    }



  }
}

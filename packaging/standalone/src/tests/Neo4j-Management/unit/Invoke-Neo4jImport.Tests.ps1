$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Invoke-Neo4jImport" {

    Context "Nodes and Relationships as comma delimited list" {
      # Commands from the command line come through as System.Object[]
      # These commands can be simulated through crafting an appropriate array
      
      # neo4j-import --into c:\graphdb --nodes "file1,file2" -relationships "file3,file4"
      $testCommand = @('--into','C:\graph\db','--nodes', @('file1','file2'),'--relationships', @('file3','file4'))

      Mock Invoke-Neo4jUtility { return 2 }
      Mock Invoke-Neo4jUtility -Verifiable { return 0} -ParameterFilter {
        $Command -eq 'Import' `
        -and $CommandArgs[0] -eq '--into' `
        -and $CommandArgs[1] -eq 'C:\graph\db' `
        -and $CommandArgs[2] -eq '--nodes' `
        -and $CommandArgs[3] -eq 'file1,file2' `
        -and $CommandArgs[4] -eq '--relationships' `
        -and $CommandArgs[5] -eq 'file3,file4'
      }

      $result = Invoke-Neo4jImport -CommandArgs $testCommand
      It "Should return exit code 0" {
        $result | Should Be 0
      }

      It "Should call verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Nodes as a single file" {
      # Commands from the command line come through as System.Object[]
      # These commands can be simulated through crafting an appropriate array

      # neo4j-import --into c:\graphdb --nodes singlefile
      $testCommand = @('--into','C:\graph\db','--nodes', 'singlefile')

      Mock Invoke-Neo4jUtility { return 2 }
      Mock Invoke-Neo4jUtility -Verifiable { return 0} -ParameterFilter {
        $Command -eq 'Import' `
        -and $CommandArgs[0] -eq '--into' `
        -and $CommandArgs[1] -eq 'C:\graph\db' `
        -and $CommandArgs[2] -eq '--nodes' `
        -and $CommandArgs[3] -eq 'singlefile'
      }

      $result = Invoke-Neo4jImport -CommandArgs $testCommand
      It "Should return exit code 0" {
        $result | Should Be 0
      }

      It "Should call verified mocks" {
        Assert-VerifiableMocks
      }
    }

  }
}

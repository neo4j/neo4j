# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.",".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
.$common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Invoke-Neo4jAdmin" {

    Context "Nodes and Relationships as comma delimited list" {
      # Commands from the command line come through as System.Object[]
      # These commands can be simulated through crafting an appropriate array

      # neo4j-admin import --mode=database --fake "file1,file2"
      $testCommand = @('import','--setting=value','--fake',@('file1','file2'))

      Mock Invoke-Neo4jUtility { Write-Host $CommandArgs -ForegroundColor Magenta; return 2 }
      Mock Invoke-Neo4jUtility -Verifiable { return 0 } -ParameterFilter {
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

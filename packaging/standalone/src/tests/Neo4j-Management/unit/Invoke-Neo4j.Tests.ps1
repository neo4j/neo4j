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
  Describe "Invoke-Neo4j" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' }
    Mock Set-Neo4jEnv {}
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
    Mock Start-Neo4jServer { 2 } -ParameterFilter { $Console -eq $true }
    Mock Start-Neo4jServer { 3 } -ParameterFilter { $Service -eq $true }
    Mock Stop-Neo4jServer { 4 }
    Mock Get-Neo4jStatus { 6 }
    Mock Install-Neo4jServer { 7 }
    Mock Uninstall-Neo4jServer { 8 }

    Context "No arguments" {
      $result = Invoke-Neo4j

      It "returns 1 if no arguments" {
        $result | Should Be 1
      }
    }

    # Helper functions - error
    Context "Helper function throws an error" {
      Mock Get-Neo4jStatus { throw "error" }

      It "returns non zero exit code on error" {
        Invoke-Neo4j 'status' -ErrorAction SilentlyContinue | Should Be 1
      }

      It "throws error when terminating error" {
        { Invoke-Neo4j 'status' -ErrorAction Stop } | Should Throw
      }
    }


    # Helper functions
    Context "Helper functions" {
      It "returns exitcode from console command" {
        Invoke-Neo4j 'console' | Should Be 2
      }

      It "returns exitcode from start command" {
        Invoke-Neo4j 'start' | Should Be 3
      }

      It "returns exitcode from stop command" {
        Invoke-Neo4j 'stop' | Should Be 4
      }

      It "returns exitcode from restart command" {
        Mock Start-Neo4jServer { 5 } -ParameterFilter { $Service -eq $true }
        Mock Stop-Neo4jServer { 0 }

        Invoke-Neo4j 'restart' | Should Be 5
      }

      It "returns exitcode from status command" {
        Invoke-Neo4j 'status' | Should Be 6
      }

      It "returns exitcode from install-service command" {
        Invoke-Neo4j 'install-service' | Should Be 7
      }

      It "returns exitcode from uninstall-service command" {
        Invoke-Neo4j 'uninstall-service' | Should Be 8
      }
    }

  }
}

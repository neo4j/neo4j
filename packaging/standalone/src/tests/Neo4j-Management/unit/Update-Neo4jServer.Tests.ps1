# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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
  Describe "Update-Neo4jServer" {

    # Setup mocking environment
    # Mock Java environment
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
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Confirm-JavaVersion { $true }
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "throws if invalid or missing neo4j directory" {
        { Update-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Non-existing service" {
      Mock Get-Service -Verifiable { return $null }
      $serverObject = global:New-MockNeo4jInstall
      $result = Update-Neo4jServer -Neo4jServer $serverObject

      It "returns 1 for service that does not exist" {
        $result | Should Be 1
        Assert-VerifiableMocks
      }
    }

    Context "Update service failure" {
      Mock Get-Service -Verifiable { return "Fake service" }
      Mock Invoke-ExternalCommand -Verifiable { throw "Error reconfiguring" }
      $serverObject = global:New-MockNeo4jInstall

      It "throws when update encounters an error" {
        { Update-Neo4jServer -Neo4jServer $serverObject } | Should Throw
        Assert-VerifiableMocks
      }
    }

    Context "Update service success" {
      Mock Get-Service -Verifiable { return "Fake service" }
      Mock Invoke-ExternalCommand -Verifiable { @{ 'exitCode' = 0 } }
      $serverObject = global:New-MockNeo4jInstall
      $result = Update-Neo4jServer -Neo4jServer $serverObject

      It "returns 0 when successfully updated" {
        $result | Should Be 0
        Assert-VerifiableMocks
      }
    }

  }
}

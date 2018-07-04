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
  Describe "Install-Neo4jServer" {

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
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Confirm-JavaVersion { $true }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "return throw if invalid or missing neo4j directory" {
        { Install-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Invalid or missing servicename in specified neo4j installation" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "return throw if invalid or missing service name" {
        { Install-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Windows service already exists" {
      Mock Get-Service -Verifiable { return 'Service Exists' }

      $serverObject = global:New-MockNeo4jInstall

      $result = Install-Neo4jServer -Neo4jServer $serverObject

      It "returns 0 for service that already exists" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service with failure" {
      Mock Get-Service { return $null }
      Mock Invoke-ExternalCommand -Verifiable { throw "Error installing" }

      $serverObject = global:New-MockNeo4jInstall

      It "throws when error during installation" {
        { Install-Neo4jServer -Neo4jServer $serverObject } | Should Throw
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service with success" {
      Mock Get-Service { return $null }
      Mock Invoke-ExternalCommand -Verifiable { @{ 'exitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Install-Neo4jServer -Neo4jServer $serverObject

      It "returns 0 when succesfully installed" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}

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
  Describe "Start-Neo4jServer" {

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
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "throws error for an invalid server object - Server" {
        { Start-Neo4jServer -Server -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }

      It "throws error for an invalid server object - Console" {
        { Start-Neo4jServer -Console -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    # Windows Service Tests
    Context "Missing service name in configuration files" {
      Mock Start-Service {}

      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "throws error for missing service name in configuration file" {
        { Start-Neo4jServer -Service -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Start service failed" {
      Mock Get-Service { return 'service' }
      Mock Invoke-ExternalCommand { throw "Should not invoke" }
      Mock Invoke-ExternalCommand -Verifiable { @{ exitCode = 1; capturedOutput = 'failed to start' } } -ParameterFilter { $Command -like '*prunsrv*.exe' }

      $serverObject = global:New-MockNeo4jInstall

      $result = Start-Neo4jServer -Service -Neo4jServer $serverObject

      It "result is 1" {
        $result | Should Be 1
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Start service succesfully" {
      Mock Get-Service { return 'service' }
      Mock Invoke-ExternalCommand { throw "Should not invoke" }
      Mock Invoke-ExternalCommand -Verifiable { @{ exitCode = 0 } } -ParameterFilter { $Command -like '*prunsrv*.exe' }

      $serverObject = global:New-MockNeo4jInstall

      $result = Start-Neo4jServer -Service -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    # Console Tests
    Context "Start as a process and missing Java" {
      Mock Get-Java {}
      Mock Start-Process {}

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
          'Home' = 'TestDrive:\some-dir-that-doesnt-exist';
          'ServerVersion' = '3.0';
          'ServerType' = 'Enterprise';
          'DatabaseMode' = '';
        })
      It "throws error if missing Java" {
        { Start-Neo4jServer -Console -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }
  }
}

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
  Describe "Uninstall-Neo4jServer" {
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
    # Mock service and process handlers
    Mock Get-Service { @{ 'State' = 'Running' } } -ParameterFilter { $Name = $global:mockServiceName }
    Mock Confirm-JavaVersion { $true }
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }
    Mock Stop-Service { $true } -ParameterFilter { $Name -eq $global:mockServiceName }

    Context "Missing service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService $null

      It "throws error for missing service name in configuration file" {
        { Uninstall-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Windows service does not exist" {
      Mock Get-Service -Verifiable { $null }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Uninstall windows service successfully" {
      Mock Invoke-ExternalCommand { @{ 'exitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
    }

    Context "During uninstall, does not stop service if already stopped" {
      Mock Get-Service { @{ 'Status' = 'Stopped' } }
      Mock Invoke-ExternalCommand { @{ 'exitCode' = 0 } }

      $serverObject = global:New-MockNeo4jInstall

      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }

      It "does not call Stop-Service" {
        Assert-MockCalled Stop-Service -Times 0
      }
    }
  }
}

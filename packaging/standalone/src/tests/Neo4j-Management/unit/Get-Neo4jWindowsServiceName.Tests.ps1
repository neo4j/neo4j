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
  Describe "Get-Neo4jWindowsServiceName" {

    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Set-Neo4jEnv {}

    Context "Missing service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "throws error for missing service name in configuration file" {
        { Get-Neo4jWindowsServiceName -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall

      It "returns Service name in configuration file" {
        Get-Neo4jWindowsServiceName -Neo4jServer $serverObject | Should be $global:mockServiceName
      }
    }
  }
}

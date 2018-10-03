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
  Describe "Get-Neo4jServer" {
    Mock Set-Neo4jEnv {}

    Context "Missing Neo4j installation" {
      Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }

      It "throws an error if no default home" {
        { Get-Neo4jServer -ErrorAction Stop } | Should Throw
      }
    }

    Context "Invalid Neo4j Server detection" {
      $mockServer = global:New-MockNeo4jInstall -IncludeFiles:$false

      It "throws an error if the home is not complete" {
        { Get-Neo4jServer -Neo4jHome $mockServer.Home -ErrorAction Stop } | Should Throw
      }
    }

    Context "Pipes and aliases" {
      $mockServer = global:New-MockNeo4jInstall
      It "processes piped paths" {
        $neoServer = ($mockServer.Home | Get-Neo4jServer)

        ($neoServer -ne $null) | Should Be $true
      }

      It "uses the Home alias" {
        $neoServer = (Get-Neo4jServer -Home $mockServer.Home)

        ($neoServer -ne $null) | Should Be $true
      }
    }

    Context "Valid Enterprise Neo4j installation" {
      $mockServer = global:New-MockNeo4jInstall -ServerType 'Enterprise' -ServerVersion '99.99' -DatabaseMode 'Arbiter'

      $neoServer = Get-Neo4jServer -Neo4jHome $mockServer.Home -ErrorAction Stop

      It "detects an enterprise edition" {
        $neoServer.ServerType | Should Be "Enterprise"
      }
      It "detects correct version" {
        $neoServer.ServerVersion | Should Be "99.99"
      }
      It "detects correct database mode" {
        $neoServer.DatabaseMode | Should Be "Arbiter"
      }
    }

    Context "Valid Community Neo4j installation" {
      $mockServer = global:New-MockNeo4jInstall -ServerType 'Community' -ServerVersion '99.99'

      $neoServer = Get-Neo4jServer -Neo4jHome $mockServer.Home -ErrorAction Stop

      It "detects a community edition" {
        $neoServer.ServerType | Should Be "Community"
      }
      It "detects correct version" {
        $neoServer.ServerVersion | Should Be "99.99"
      }
    }

    Context "Valid Community Neo4j installation with relative paths" {
      $mockServer = global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j' -ServerType 'Community' -ServerVersion '99.99'

      # Get the absolute path
      $Neo4jDir = (Get-Item $mockServer.Home).FullName.TrimEnd('\')

      It "detects correct home path using double dot" {
        $neoServer = Get-Neo4jServer -Neo4jHome "$($mockServer.Home)\lib\.." -ErrorAction Stop
        $neoServer.Home | Should Be $Neo4jDir
      }

      It "detects correct home path using single dot" {
        $neoServer = Get-Neo4jServer -Neo4jHome "$($mockServer.Home)\." -ErrorAction Stop
        $neoServer.Home | Should Be $Neo4jDir
      }

      It "detects correct home path ignoring trailing slash" {
        $neoServer = Get-Neo4jServer -Neo4jHome "$($mockServer.Home)\" -ErrorAction Stop
        $neoServer.Home | Should Be $Neo4jDir
      }
    }

    Context "No explicit location for config directory is provided" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      $Neo4jDir = (Get-Item 'TestDrive:\neo4j').FullName.TrimEnd('\')

      It "Defaults config path to $Neo4jDir\conf" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        $neoServer.ConfDir | Should Be (Join-Path -Path $Neo4jDir -ChildPath 'conf')
      }
    }

    Context "NEO4J_CONF environment variable is set" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      Mock Get-Neo4jEnv { 'TestDrive:\neo4j-conf' } -ParameterFilter { $Name -eq 'NEO4J_CONF' }

      It "Gets conf directory from environment variable" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        $neoServer.ConfDir | Should Be 'TestDrive:\neo4j-conf'
      }
    }

    Context "NEO4J_HOME environment variable is not set" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      Mock Get-Neo4jEnv {} -ParameterFilter { $Name -eq 'NEO4J_HOME' }

      It "Creates NEO4J_HOME if not set" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        Assert-MockCalled Set-Neo4jEnv -Times 1 -ParameterFilter { $Name -eq 'NEO4J_HOME' }
      }
    }

    Context "NEO4J_HOME environment variable is already set" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      Mock Get-Neo4jEnv { 'TestDrive:\bad-location' } -ParameterFilter { $Name -eq 'NEO4J_HOME' }

      It "Does not modify NEO4J_HOME if already set" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        Assert-MockCalled Set-Neo4jEnv -Times 0 -ParameterFilter { $Name -eq 'NEO4J_HOME' }
      }
    }

    Context "Deprecation warning if a neo4j-wrapper.conf file is found" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      Mock Write-Warning

      '# Mock File' | Out-File 'TestDrive:\neo4j\conf\neo4j-wrapper.conf'

      It "Should raise a warning if conf\neo4j-wrapper.conf exists" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        Assert-MockCalled Write-Warning -Times 1
      }
    }

    Context "Log directory doesn't exist" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      Mock Get-Neo4jEnv { 'TestDrive:\neo4j\logs' } -ParameterFilter { $Name -eq 'NEO4J_LOGS' }

      It "Should create it" {
        $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
        Test-Path -PathType Container $neoServer.LogDir | Should Be $true
      }
    }
  }
}

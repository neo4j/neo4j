$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Get-Neo4jServer" {

    Context "Missing Neo4j installation" {
      It "throws an error if no default home" {
         { Get-Neo4jServer -ErrorAction Stop } | Should Throw       
      }
    }

    Context "Invalid Neo4j installation" {
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
        $neoServer = ( $mockServer.Home | Get-Neo4jServer )

        ($neoServer -ne $null) | Should Be $true
      }
  
      It "uses the Home alias" {
        $neoServer = ( Get-Neo4jServer -Home $mockServer.Home )
        
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
  }
}

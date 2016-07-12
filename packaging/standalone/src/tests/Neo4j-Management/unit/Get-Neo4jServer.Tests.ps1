$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Get-Neo4jServer" {

    Context "Missing Neo4j installation" {
      Mock Get-Neo4jHome { return }
  
      It "throws an error if no default home" {
         { Get-Neo4jServer -ErrorAction Stop } | Should Throw       
      }
      It "attempts to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 1
      }    
    }

    Context "Invalid Neo4j installation" {
      Mock Get-Neo4jHome { return "TestDrive:\SomePath" }
      Mock Confirm-Neo4jHome { return $false }
  
      It "throws an error if no default home" {
         { Get-Neo4jServer -ErrorAction Stop } | Should Throw       
      }
      It "attempts to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 1
      }
      It "attempts to confirm the home" {
        Assert-MockCalled Confirm-Neo4jHome -Times 1
      }    
    }

    Context "Invalid Neo4j Server detection" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j' -IncludeFiles:$false

      It "throws an error if the home is not complete" {
         { Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j' -ErrorAction Stop } | Should Throw       
      }
    }
    
    Context "Pipes and aliases" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j'
      It "processes piped paths" {
        $neoServer = ( 'TestDrive:\neo4j' | Get-Neo4jServer )

        ($neoServer -ne $null) | Should Be $true
      }
  
      It "uses the Home alias" {
        $neoServer = ( Get-Neo4jServer -Home 'TestDrive:\neo4j' )
        
        ($neoServer -ne $null) | Should Be $true
      }
    }
    
    Context "Valid Enterprise Neo4j installation" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j-ent' -ServerType 'Enterprise' -ServerVersion '99.99'

      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j-ent' -ErrorAction Stop

      It "detects an enterprise edition" {
         $neoServer.ServerType | Should Be "Enterprise"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
    }

    Context "Valid Advanced Neo4j installation" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j-adv' -ServerType 'Advanced' -ServerVersion '99.99'
     
      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j-adv' -ErrorAction Stop
  
      It "detects an advanced edition" {
         $neoServer.ServerType | Should Be "Advanced"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
    }

    Context "Valid Community Neo4j installation" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j-com' -ServerType 'Community' -ServerVersion '99.99'

      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j-com' -ErrorAction Stop
  
      It "detects a community edition" {
         $neoServer.ServerType | Should Be "Community"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
    }

    Context "Valid Community Neo4j installation with relative paths" {
      global:New-MockNeo4jInstall -RootDir 'TestDrive:\neo4j' -ServerType 'Community' -ServerVersion '99.99'

      # Get the absolute path
      $Neo4jDir = (Get-Item 'TestDrive:\neo4j').FullName.TrimEnd('\')

      It "detects correct home path using double dot" {
         $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\system\..' -ErrorAction Stop
         $neoServer.Home | Should Be $Neo4jDir      
      }

      It "detects correct home path using single dot" {
         $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\.' -ErrorAction Stop
         $neoServer.Home | Should Be $Neo4jDir      
      }

      It "detects correct home path ignoring trailing slash" {
         $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\neo4j\' -ErrorAction Stop
         $neoServer.Home | Should Be $Neo4jDir      
      }
    }
  }
}

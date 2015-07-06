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
      Mock Get-Neo4jHome { return  }
      Mock Confirm-Neo4jHome { return $true }
      Mock Confirm-Neo4jServerObject { return $false }
      Mock Get-ChildItem { return } 
      
      It "throws an error if no default home" {
         { Get-Neo4jServer -Neo4jHome 'TestDrive:\SomePath' -ErrorAction Stop } | Should Throw       
      }
      It "does not attempt to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 0
      }
      It "attempts to validate the home" {
        Assert-MockCalled Confirm-Neo4jHome -Times 1
      }    
      It "attempts to validate the server details" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }    
    }
    
    Context "Pipes and aliases" {
      It "processes piped paths" {
        Mock Get-Neo4jHome { return }
        Mock Confirm-Neo4jHome { return $true }
        Mock Confirm-Neo4jServerObject { return $true }
        Mock Get-ChildItem { return }
  
        $neoServer = ( 'TestDrive:\SomePath' | Get-Neo4jServer )

        ($neoServer -ne $null) | Should Be $true
      }
  
      It "uses the Home alias" {
        Mock Get-Neo4jHome { return }
        Mock Confirm-Neo4jHome { return $true }
        Mock Confirm-Neo4jServerObject { return $true }
        Mock Get-ChildItem { return }
  
        $neoServer = ( Get-Neo4jServer -Home 'TestDrive:\SomePath' )
        
        ($neoServer -ne $null) | Should Be $true
      }
    }
    
    Context "Valid Enterprise Neo4j installation" {
      Mock Get-Neo4jHome { return }
      Mock Confirm-Neo4jHome { return $true }
      Mock Confirm-Neo4jServerObject { return $true }
      Mock Get-ChildItem { return (@{ 'Name' = 'neo4j-server-enterprise-99.99.jar'},@{ 'Name' = 'neo4j-server-99.99.jar'}) }
      
      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\SomePath' -ErrorAction Stop
  
      It "does not attempt to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 0
      }
      It "attempts to validate the home" {
        Assert-MockCalled Confirm-Neo4jHome -Times 1
      }    
      It "attempts to validate the server details" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }    
      It "detects an enterprise edition" {
         $neoServer.ServerType | Should Be "Enterprise"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
      It "detects correct home path" {
         $neoServer.Home | Should Be 'TestDrive:\SomePath'
      }
    }

    Context "Valid Advanced Neo4j installation" {
      Mock Get-Neo4jHome { return }
      Mock Confirm-Neo4jHome { return $true }
      Mock Confirm-Neo4jServerObject { return $true }
      Mock Get-ChildItem { return (@{ 'Name' = 'neo4j-server-advanced-99.99.jar'},@{ 'Name' = 'neo4j-server-99.99.jar'}) }
      
      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\SomePath' -ErrorAction Stop
  
      It "does not attempt to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 0
      }
      It "attempts to validate the home" {
        Assert-MockCalled Confirm-Neo4jHome -Times 1
      }    
      It "attempts to validate the server details" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }    
      It "detects an advanced edition" {
         $neoServer.ServerType | Should Be "Advanced"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
      It "detects correct home path" {
         $neoServer.Home | Should Be 'TestDrive:\SomePath'
      }
    }

    Context "Valid Community Neo4j installation" {
      Mock Get-Neo4jHome { return }
      Mock Confirm-Neo4jHome { return $true }
      Mock Confirm-Neo4jServerObject { return $true }
      Mock Get-ChildItem { return @{ 'Name' = 'neo4j-server-99.99.jar'} }
      
      $neoServer = Get-Neo4jServer -Neo4jHome 'TestDrive:\SomePath' -ErrorAction Stop
  
      It "does not attempt to get the default home" {
        Assert-MockCalled Get-Neo4jHome -Times 0
      }
      It "attempts to validate the home" {
        Assert-MockCalled Confirm-Neo4jHome -Times 1
      }    
      It "attempts to validate the server details" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }    
      It "detects a community edition" {
         $neoServer.ServerType | Should Be "Community"      
      }
      It "detects correct version" {
         $neoServer.ServerVersion | Should Be "99.99"      
      }
      It "detects correct home path" {
         $neoServer.Home | Should Be 'TestDrive:\SomePath'
      }
    }
  }
}

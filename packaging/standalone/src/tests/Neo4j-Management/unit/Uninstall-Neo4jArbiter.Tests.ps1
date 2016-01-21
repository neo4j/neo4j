$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Uninstall-Neo4jArbiter" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Uninstall-Neo4jArbiter
      
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }

    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Uninstall-Neo4jArbiter -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
      It "return null if invalid directory" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }

    Context "Invalid or missing server object" {
      Mock Confirm-Neo4jServerObject { return $false }
      
      It "throws error for an invalid server object" {
        { Uninstall-Neo4jArbiter -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }

    Context "Throws error for Community Edition" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    

      It "throws error if community edition" {
        { Stop-Neo4jArbiter -ErrorAction Stop } | Should Throw
      }
    }


    Context "Windows service does not exist" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-WmiObject -Verifiable { return $null }

      It "throws error for missing service name in configuration file" {
        { Uninstall-Neo4jArbiter -ErrorAction Stop } | Should Throw
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Windows service does not exist but no error" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-WmiObject -Verifiable { return $null }

      $result = Uninstall-Neo4jArbiter -SucceedIfNotExist

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Uninstall windows service" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Stop-Service { return 1 } -ParameterFilter { $Name -eq 'Neo4jArbiter'}
      Mock Get-WmiObject -Verifiable {
        # Mock a Win32_Service WMI object
        $mock = New-Object -TypeName PSCustomObject
        $mock | Add-Member -MemberType ScriptMethod -Name 'delete' -Value { return 2 } | Out-Null
        
        return $mock
      } -ParameterFilter { $Filter -eq "Name='Neo4jArbiter'" }

      $result = Uninstall-Neo4jArbiter

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}

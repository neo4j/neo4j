$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Stop-Neo4jArbiter" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Stop-Neo4jArbiter
      
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }

    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Stop-Neo4jArbiter -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
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
        { Stop-Neo4jArbiter -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }

    Context "Throws error for Community Edition" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return $null }

      It "throws error if community edition" {
        { Stop-Neo4jArbiter -ErrorAction Stop } | Should Throw
      }
    }

    Context "Missing service name in configuration files" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return $null }

      It "throws error for missing service name in configuration file" {
        { Stop-Neo4jArbiter -ErrorAction Stop } | Should Throw
      }
      
      It "calls Get-Neo4jSetting" {
        Assert-MockCalled Get-Neo4jSetting -Times 1
      }
    }

    Context "Stop service by name in configuration file" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Stop-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeServiceName'}
      
      $result = Stop-Neo4jArbiter

      It "result is exit code" {
        $result | Should Be 1
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Stop service by named parameter" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Stop-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeOtherServiceName'}
      
      $result = Stop-Neo4jArbiter -ServiceName 'SomeOtherServiceName'

      It "result is exit code" {
        $result | Should Be 1
      }

      It "does not call Get-Neo4jSetting" {
        Assert-MockCalled Get-Neo4jSetting -Times 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Stop service and passthru server object" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Stop-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeServiceName'}
      
      $result = Stop-Neo4jArbiter -PassThru

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

  }
}
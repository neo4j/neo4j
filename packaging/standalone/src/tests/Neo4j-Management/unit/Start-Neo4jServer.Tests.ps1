$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Start-Neo4jServer" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Start-Neo4jServer
      
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }

    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Start-Neo4jServer -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
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
        { Start-Neo4jServer -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }
    
    # Windows Service Tests
    Context "Missing service name in configuration files" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return $null }

      It "throws error for missing service name in configuration file" {
        { Start-Neo4jServer -ErrorAction Stop } | Should Throw
      }
      
      It "calls Get-Neo4jSetting" {
        Assert-MockCalled Get-Neo4jSetting -Times 1
      }
    }    

    Context "Start service by name in configuration file" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Start-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeServiceName'}
      
      $result = Start-Neo4jServer

      It "result is exit code" {
        $result | Should Be 1
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Start service by named parameter" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Start-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeOtherServiceName'}
      
      $result = Start-Neo4jServer -ServiceName 'SomeOtherServiceName'

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

    Context "Start service and passthru server object" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Start-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeServiceName'}
      
      $result = Start-Neo4jServer -PassThru

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    # Console Tests
    Context "Start as a process and missing Java" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Java { }
      Mock Start-Process { }

      It "throws error if missing Java" {
        { Start-Neo4jServer -Console -ErrorAction Stop } | Should Throw
      }
    }
    
    Context "Start as a process without Wait" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Java -Verifiable { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Start-Process { }
      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter {
        (-not $Wait) -and ($FilePath -eq 'java.exe') -and (($ArgumentList -join ' ') -eq 'arg1 arg2')
      }
      
      $result = Start-Neo4jServer -Console

      It "result is exit code" {
        $result | Should Be 1
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Start as a process with Wait" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Java -Verifiable { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Start-Process { }
      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter {
        ($Wait) -and ($FilePath -eq 'java.exe') -and (($ArgumentList -join ' ') -eq 'arg1 arg2')
      }
      
      $result = Start-Neo4jServer -Console -Wait

      It "result is exit code" {
        $result | Should Be 1
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Start as a process and passthru server object" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Java -Verifiable { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} }
      
      $result = Start-Neo4jServer -Console -PassThru

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
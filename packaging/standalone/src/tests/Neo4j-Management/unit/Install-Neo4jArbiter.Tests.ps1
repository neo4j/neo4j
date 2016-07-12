$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Install-Neo4jArbiter" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Install-Neo4jArbiter
      
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }

    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Install-Neo4jArbiter -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
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
        { Install-Neo4jArbiter -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
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
    
    Context "Windows service already exists" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock New-Service -Verifiable { throw 'Service already exists' }

      It "throws error for service that already exists" {
        { Install-Neo4jArbiter -ErrorAction Stop } | Should Throw
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Windows service already exists but no error" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock New-Service { throw 'Service already exists' }
      Mock Set-Neo4jSetting -Verifiable { return }
      Mock Get-Service -Verifiable { return 'service' }

     $result = Install-Neo4jArbiter -SucceedIfAlreadyExists

      It "result is a windows service" {
        $result | Should Be 'service'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Set-Neo4jSetting { throw 'Did not call Set-Neo4jSetting correctly' }
      Mock New-Service { throw 'Did not call New-Service correctly' }
      
      Mock New-Service -Verifiable { 'service' }  -ParameterFilter {
        ($Name -eq 'ServiceName') -and ($Description -eq 'ServiceDescription') -and ($DisplayName -eq 'ServerDisplayName') -and
        ($BinaryPathName -eq '"java.exe" arg1 arg2 ServiceName') -and ($StartupType -eq 'Disabled')
      }
      
      Mock Set-Neo4jSetting -Verifiable { return } -ParameterFilter { $Value -eq 'ServiceName' }

     $result = Install-Neo4jArbiter -Name 'ServiceName' -DisplayName 'ServerDisplayName' -Description 'ServiceDescription' -StartType Disabled

      It "result is a windows service" {
        $result | Should Be 'service'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service and passthru server object" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise';} }    
      Mock Get-Neo4jSetting { return @{'Value' = 'SomeServiceName'} }
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Set-Neo4jSetting { throw 'Did not call Set-Neo4jSetting correctly' }
      Mock New-Service { throw 'Did not call New-Service correctly' }
      
      Mock New-Service -Verifiable { 'service' }  -ParameterFilter {
        ($Name -eq 'ServiceName') -and ($Description -eq 'ServiceDescription') -and ($DisplayName -eq 'ServerDisplayName') -and
        ($BinaryPathName -eq '"java.exe" arg1 arg2 ServiceName') -and ($StartupType -eq 'Disabled')
      }
      Mock Set-Neo4jSetting -Verifiable { return } -ParameterFilter { $Value -eq 'ServiceName' }

     $result = Install-Neo4jArbiter -Name 'ServiceName' -DisplayName 'ServerDisplayName' -Description 'ServiceDescription' -StartType Disabled -PassThru

      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

  }
}
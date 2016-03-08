$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Install-Neo4jServer" {

    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Java { return @{ 'java' = '' } }
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
 
      It "return throw if invalid or missing neo4j directory" {
        { Install-Neo4jServer -Neo4jServer $serverObject  -ErrorAction Stop }  | Should Throw      
      }
    }

    Context "Invalid or missing servicename in specified neo4j installation" {
      Mock Get-Java { return @{ 'java' = '' } }
      Mock Get-Neo4jWindowsServiceName { throw "Could not determine Service Name" }
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
 
      It "return throw if invalid or missing service name" {
        { Install-Neo4jServer -Neo4jServer $serverObject  -ErrorAction Stop }  | Should Throw      
      }
    }

    Context "Windows service already exists" {
      Mock Get-Neo4jWindowsServiceName { return 'SomeServiceName'}
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock Get-Service -Verifiable { return 'Service Exists' }
      Mock New-Service { throw "Should not call New-Service"}

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Install-Neo4jServer -Neo4jServer $serverObject
      It "returns 0 for service that already exists" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service with failure" {
      Mock Get-Neo4jWindowsServiceName { return 'SomeServiceName'}
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock New-Service { 'fake success' }
      
      Mock New-Service -Verifiable { throw "Error installing" }  -ParameterFilter {
        ($Name -eq 'SomeServiceName') -and
        ($BinaryPathName -eq '"java.exe" arg1 arg2 SomeServiceName') -and
        ($StartupType -eq 'Automatic')
      }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = 

      It "throws when error during installation" {
        { Install-Neo4jServer -Neo4jServer $serverObject } | Should Throw
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Installs windows service with success" {
      Mock Get-Neo4jWindowsServiceName { return 'SomeServiceName'}
      Mock Get-Java { return @{ 'java' = 'java.exe'; 'args' = @('arg1','arg2') }}
      Mock New-Service { throw 'Did not call New-Service correctly' }
      
      Mock New-Service -Verifiable { 'service' }  -ParameterFilter {
        ($Name -eq 'SomeServiceName') -and
        ($BinaryPathName -eq '"java.exe" arg1 arg2 SomeServiceName') -and
        ($StartupType -eq 'Automatic')
      }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Install-Neo4jServer -Neo4jServer $serverObject

      It "returns 0 when succesfully installed" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

  }
}

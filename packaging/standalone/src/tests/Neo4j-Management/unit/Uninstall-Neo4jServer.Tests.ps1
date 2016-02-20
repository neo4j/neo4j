$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Uninstall-Neo4jServer" {

    Context "Missing service name in configuration files" {
      Mock Get-Neo4jWindowsServiceName { throw "Invalid service name" }
      Mock Get-WmiObject { }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      

      It "throws error for missing service name in configuration file" {
        { Uninstall-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
      
      It "calls Get-Neo4jSetting" {
        Assert-MockCalled Get-Neo4jWindowsServiceName -Times 1
      }
    }

    Context "Windows service does not exist" {
      Mock Get-Neo4jWindowsServiceName -Verifiable { 'SomeServiceName' }
      Mock Get-WmiObject -Verifiable { return $null }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Uninstall windows service successfully" {
      Mock Get-Neo4jWindowsServiceName -Verifiable { 'SomeServiceName' }
      Mock Stop-Service { throw "Did not call Stop-Service correctly" }
      Mock Get-WMIObject { throw "Did not call WMI Object correctly" }
      
      Mock Stop-Service -Verifiable { return 1 } -ParameterFilter { $Name -eq 'SomeServiceName'}
      Mock Get-WmiObject -Verifiable {
        # Mock a Win32_Service WMI object
        $mock = New-Object -TypeName PSCustomObject
        $mock | Add-Member -MemberType ScriptMethod -Name 'delete' -Value { return 2 } | Out-Null
        $mock | Add-Member -MemberType NoteProperty -Name 'State' -Value 'Running'
        return $mock
      } -ParameterFilter { $Filter -eq "Name='SomeServiceName'" }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
    
    Context "During uninstall, does not stop service if already stopped" {
      Mock Get-Neo4jWindowsServiceName -Verifiable { 'SomeServiceName' }
      Mock Get-WMIObject { throw "Did not call WMI Object correctly" }
      
      Mock Stop-Service  { } 
      Mock Get-WmiObject -Verifiable {
        # Mock a Win32_Service WMI object
        $mock = New-Object -TypeName PSCustomObject
        $mock | Add-Member -MemberType ScriptMethod -Name 'delete' -Value { return 2 } | Out-Null
        $mock | Add-Member -MemberType NoteProperty -Name 'State' -Value 'Stopped'
        return $mock
      } -ParameterFilter { $Filter -eq "Name='SomeServiceName'" }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Uninstall-Neo4jServer -Neo4jServer $serverObject

      It "result is 0" {
        $result | Should Be 0
      }
      
      It "does not call Stop-Service" {
        Assert-MockCalled Stop-Service -Times 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
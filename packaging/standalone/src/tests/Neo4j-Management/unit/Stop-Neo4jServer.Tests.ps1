$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Stop-Neo4jServer" {

    Context "Missing service name in configuration files" {
      Mock Get-Neo4jWindowsServiceName { throw "Missing service name" }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      It "throws error for missing service name in configuration file" {
        { Stop-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
      
      It "calls Get-Neo4jWindowsServiceName" {
        Assert-MockCalled Get-Neo4jWindowsServiceName -Times 1
      }
    }

    Context "Stop service succesfully but didn't stop" {
      Mock Get-Neo4jWindowsServiceName { 'SomeServiceName' }
      Mock Stop-Service { throw "Called Stop-Service incorrectly"}
      Mock Stop-Service -Verifiable { @{ Status = 'Stop Pending'} } -ParameterFilter { $Name -eq 'SomeServiceName'}      

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Stop-Neo4jServer -Neo4jServer $serverObject
      It "result is 2" {
        $result | Should Be 2
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Stop service succesfully" {
      Mock Get-Neo4jWindowsServiceName { 'SomeServiceName' }
      Mock Stop-Service { throw "Called Stop-Service incorrectly"}
      Mock Stop-Service -Verifiable { @{ Status = 'Stopped'} } -ParameterFilter { $Name -eq 'SomeServiceName'}      

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Stop-Neo4jServer -Neo4jServer $serverObject
      It "result is 0" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
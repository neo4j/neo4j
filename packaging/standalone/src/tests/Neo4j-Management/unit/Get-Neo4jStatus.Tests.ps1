$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jStatus" {

    Context "Missing service name in configuration files" {
      Mock Get-Neo4jWindowsServiceName { throw "Missing service name" }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      It "throws error for missing service name in configuration file" {
        { Get-Neo4jStatus -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
      
      It "calls Get-Neo4jWindowsServiceName" {
        Assert-MockCalled Get-Neo4jWindowsServiceName -Times 1
      }
    }

    Context "Service not installed" {
      Mock Get-Neo4jWindowsServiceName { 'SomeServiceName' }
      Mock Get-Service -Verifiable { throw "Missing Service"}

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Get-Neo4jStatus -Neo4jServer $serverObject
      It "result is 3" {
        $result | Should Be 3
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Service not installed but not running" {
      Mock Get-Neo4jWindowsServiceName { 'SomeServiceName' }
      Mock Get-Service -Verifiable { @{ Status = 'Stopped' }}

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Get-Neo4jStatus -Neo4jServer $serverObject
      It "result is 3" {
        $result | Should Be 3
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Service not installed running" {
      Mock Get-Neo4jWindowsServiceName { 'SomeServiceName' }
      Mock Get-Service -Verifiable { @{ Status = 'Running' }}

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      $result = Get-Neo4jStatus -Neo4jServer $serverObject
      It "result is 0" {
        $result | Should Be 0
      }
      
      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
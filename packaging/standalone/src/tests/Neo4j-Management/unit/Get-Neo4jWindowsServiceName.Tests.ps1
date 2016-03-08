$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jWindowsServiceName" {


    Context "Missing service name in configuration files" {
      Mock Get-Neo4jSetting { throw "Missing service name" }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      It "throws error for missing service name in configuration file" {
        { Get-Neo4jWindowsServiceName -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Service name in configuration files" {
      Mock Get-Neo4jSetting { @{ Value = "ServiceName"} }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' =  'TestDrive:\some-dir-that-doesnt-exist';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })      
      It "returns Service name in configuration file" {
        Get-Neo4jWindowsServiceName -Neo4jServer $serverObject | Should be 'ServiceName'
      }
    }

  }
}
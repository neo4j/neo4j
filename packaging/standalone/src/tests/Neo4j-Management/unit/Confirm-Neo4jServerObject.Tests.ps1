$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Confirm-Neo4jServerObject" {
    Context "Invalid Server Object" {
      It "throw if missing object" {
        { Confirm-Neo4jServerObject } | Should Throw
      }
      It "return false if missing all properties" {
        (Confirm-Neo4jServerObject -Neo4jServer (New-Object -TypeName PSCustomObject)) | Should be $false
      }
      It "return false if Home is null" {
        $serverObject = New-Object -TypeName PSCustomObject -Property @{
          'Home' = $null;
          'ServerVersion' = '99.99';
          'ServerType' = 'Community';
        }
        (Confirm-Neo4jServerObject -Neo4jServer $serverObject) | Should be $false
      }
      It "return false if ServerVersion is null" {
        $serverObject = New-Object -TypeName PSCustomObject -Property @{
          'Home' = 'TestDrive:\SomePath';
          'ServerVersion' = $null;
          'ServerType' = 'Community';
        }
        (Confirm-Neo4jServerObject -Neo4jServer $serverObject) | Should be $false
      }
      It "return false if ServerType is null" {
        $serverObject = New-Object -TypeName PSCustomObject -Property @{
          'Home' = 'TestDrive:\SomePath';
          'ServerVersion' = '99.99';
          'ServerType' = $null;
        }
        (Confirm-Neo4jServerObject -Neo4jServer $serverObject) | Should be $false
      }
      It "return false if ServerType is not Community, Advanced or Enterprise" {
        $serverObject = New-Object -TypeName PSCustomObject -Property @{
          'Home' = 'TestDrive:\SomePath';
          'ServerVersion' = '99.99';
          'ServerType' = 'SomethingSilly';
        }
        (Confirm-Neo4jServerObject -Neo4jServer $serverObject) | Should be $false
      }
      It "return false if Home does not exist" {
        $serverObject = New-Object -TypeName PSCustomObject -Property @{
          'Home' = 'TestDrive:\SomePath';
          'ServerVersion' = '99.99';
          'ServerType' = 'Community';
        }
        (Confirm-Neo4jServerObject -Neo4jServer $serverObject) | Should be $false
      }
    }
  
    Context "Valid Community Server Object" {
      # Setup
      $neo4jPath = 'TestDrive:\Neo4j'
      Mock Test-Path { $true }  -ParameterFilter { $Path -eq $neo4jPath }
      $serverObject = New-Object -TypeName PSCustomObject -Property @{
        'Home' = $neo4jPath;
        'ServerVersion' = '99.99';
        'ServerType' = 'Community';
      }
      $result = Confirm-Neo4jServerObject -Neo4jServer $serverObject
  
      It "returns true" {
        $result | Should be $true
      }
      It "attemtps to validate the path" {
        Assert-MockCalled Test-Path -Times 1
      }
    }

    Context "Valid Advanced Server Object" {
      # Setup
      $neo4jPath = 'TestDrive:\Neo4j'
      Mock Test-Path { $true }  -ParameterFilter { $Path -eq $neo4jPath }
      $serverObject = New-Object -TypeName PSCustomObject -Property @{
        'Home' = $neo4jPath;
        'ServerVersion' = '99.99';
        'ServerType' = 'Advanced';
      }
      $result = Confirm-Neo4jServerObject -Neo4jServer $serverObject
  
      It "returns true" {
        $result | Should be $true
      }
      It "attemtps to validate the path" {
        Assert-MockCalled Test-Path -Times 1
      }
    }

    Context "Valid Enterprise Server Object" {
      # Setup
      $neo4jPath = 'TestDrive:\Neo4j'
      Mock Test-Path { $true }  -ParameterFilter { $Path -eq $neo4jPath }
      $serverObject = New-Object -TypeName PSCustomObject -Property @{
        'Home' = $neo4jPath;
        'ServerVersion' = '99.99';
        'ServerType' = 'Enterprise';
      }
      $result = Confirm-Neo4jServerObject -Neo4jServer $serverObject
  
      It "returns true" {
        $result | Should be $true
      }
      It "attemtps to validate the path" {
        Assert-MockCalled Test-Path -Times 1
      }
    }
  }
}

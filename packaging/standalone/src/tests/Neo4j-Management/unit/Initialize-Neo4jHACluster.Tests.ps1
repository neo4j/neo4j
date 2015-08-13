$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Initialize-Neo4jHACluster" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:1'
  
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return } -ParameterFilter { $Neo4jServer -eq 'TestDrive:\some-dir-that-doesnt-exist' }
      $result = Initialize-Neo4jHACluster -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'  -ServerID 1 -InitialHosts '127.0.0.1:1'
  
      It "return null if invalid directory" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing server object" {
      Mock Confirm-Neo4jServerObject { return }
      
      It "throws error for an invalid server object" {
        { Initialize-Neo4jHACluster -Neo4jServer (New-Object -TypeName PSCustomObject)  -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }
  
    Context "Throws error for Community Edition" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      Mock Get-Neo4jSetting { return $null }

      It "throws error if community edition" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction Stop } | Should Throw
      }
    }

    Context "All default settings" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { return (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      $result = Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:1'

      It "returns at least one configuration setting" {
        ($result.Count -gt 0) | Should Be $true
      }
      It "attempts to write settings" {
        Assert-MockCalled Set-Neo4jSetting
      }
    }

    Context "Missing ServerID" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { return (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw if missing ServerID" {
        { Initialize-Neo4jHACluster -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
    }    

    Context "Missing InitialHosts" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { return (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw if missing InitialHosts" {
        { Initialize-Neo4jHACluster -ServerID 1 -ErrorAction 'Stop' } | Should Throw
      }
    }
    
    Context "ServerID Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw for bad cast (Alpha chars)" {
        { Initialize-Neo4jHACluster -ServerID 'abcd' -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad number (Negative)" {
        { Initialize-Neo4jHACluster -ServerID -1234 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad number (Zero)" {
        { Initialize-Neo4jHACluster -ServerID 0 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad number (Max + 1)" {
        { Initialize-Neo4jHACluster -ServerID 65536 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should not throw for good number (Min)" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
      It "should not throw for good number (Max)" {
        { Initialize-Neo4jHACluster -ServerID 65535 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
    }
    
    Context "InitialHosts Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw for bad IP (Alpha chars)" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts 'a.b.c.d' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad IP (Other chars)" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '1.1.1.1:1#$^' -ErrorAction 'Stop' } | Should Throw
      }
      It "should not throw for good single host" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:5000' -ErrorAction 'Stop' } | Should Not Throw
      }
      It "should not throw for good single host with port list" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:5000-5010' -ErrorAction 'Stop' } | Should Not Throw
      }
      It "should not throw for good many hosts with port list" {
        { Initialize-Neo4jHACluster -ServerID 1 -InitialHosts '127.0.0.1:5000-5010,127.0.0.1:6000-6010' -ErrorAction 'Stop' } | Should Not Throw
      }
    }

    Context "ClusterServer Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw for bad IP (Alpha chars)" {
        { Initialize-Neo4jHACluster -ClusterServer 'a.b.c.d' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad IP (Too big int)" {
        { Initialize-Neo4jHACluster -ClusterServer '2600.1.1.1' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad IP (Other chars)" {
        { Initialize-Neo4jHACluster -ClusterServer '1.1.1.1:1#$^' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should not throw for good single host" {
        { Initialize-Neo4jHACluster -ClusterServer '127.0.0.1:5000' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
      It "should not throw for good single host with port list" {
        { Initialize-Neo4jHACluster -ClusterServer '127.0.0.1:5000-5010' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
    }

    Context "HAServer Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should throw for bad IP (Alpha chars)" {
        { Initialize-Neo4jHACluster -HAServer 'a.b.c.d' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad IP (Too big int)" {
        { Initialize-Neo4jHACluster -HAServer '2600.1.1.1' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should throw for bad IP (Other chars)" {
        { Initialize-Neo4jHACluster -HAServer '1.1.1.1:1#$^' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Throw
      }
      It "should not throw for good single host" {
        { Initialize-Neo4jHACluster -HAServer '127.0.0.1:5000' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
      It "should not throw for good single host with port list" {
        { Initialize-Neo4jHACluster -HAServer '127.0.0.1:5000-5010' -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
    }

    Context "DisallowClusterInit Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      It "should not throw if specified" {
        { Initialize-Neo4jHACluster -DisallowClusterInit -ServerID 1 -InitialHosts '127.0.0.1:1' -ErrorAction 'Stop' } | Should Not Throw
      }
    }

    Context "PassThru Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Set-Neo4jSetting { Write-Output (New-Object -TypeName PSCustomObject -Property (@{'Value' = 'Something'; })) }
      
      $result = Initialize-Neo4jHACluster -DisallowClusterInit -ServerID 1 -InitialHosts '127.0.0.1:1' -PassThru
      
      It "result is Neo4j Server object" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
      
      It "result home is the same as input" {
        $result.Home | Should Be 'TestDrive:\FakeDir'      
      }
    }
  }
}
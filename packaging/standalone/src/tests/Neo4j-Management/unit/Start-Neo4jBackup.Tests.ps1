$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Start-Neo4jBackup" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { }
      Mock Start-Process { }
      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir'
  
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { } -ParameterFilter { $Neo4jServer -eq 'TestDrive:\some-dir-that-doesnt-exist' }
      Mock Start-Process { }
      $result = Start-Neo4jBackup -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist' -To 'TestDrive:\FakeDir'
  
      It "return null if invalid directory" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing server object" {
      Mock Confirm-Neo4jServerObject { return $false }
      Mock Start-Process { }
      
      It "throws error for an invalid server object" {
        { Start-Neo4jBackup -Neo4jServer (New-Object -TypeName PSCustomObject) -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }

    Context "Throws error for Community Edition" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      It "throws error if community edition" {
        { Start-Neo4jBackup -ErrorAction Stop -To 'TestDrive:\FakeDir' } | Should Throw
      }
    }

    Context "Missing Java" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { }
      Mock Start-Process { }

      It "throws error if missing Java" {
        { Start-Neo4jBackup -ErrorAction Stop } | Should Throw
      }
    }
    
    Context "Uses default values" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host 127.0.0.1') -and
        ($ArgumentList -join ' ').Contains('-port 6362') -and
        ($ArgumentList -join ' ').Contains('-to TestDrive:\FakeDir')        
      }
      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir'

      It "uses default values if nothing specified" {
        Assert-VerifiableMocks
      }

      It "returns the process exit code" {
        $result | Should Be 1
      }
    }

    Context "Backup is disabled" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='false' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      It "throws if backup is disabled" {
        { Start-Neo4jBackup -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
    }

    Context "Uses default values from configuration file" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_server'; 'Value'='1.2.3.4:1234' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host 1.2.3.4') -and
        ($ArgumentList -join ' ').Contains('-port 1234') -and
        ($ArgumentList -join ' ').Contains('-to TestDrive:\FakeDir')        
      }
      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir'

      It "uses default values if nothing specified" {
        Assert-VerifiableMocks
      }

      It "returns the process exit code" {
        $result | Should Be 1
      }
    }


    Context "UseHost Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      
      It "should throw for bad IP (Alpha chars)" {
        { Start-Neo4jBackup -UseHost 'a.b.c.d' -UsePort 6362 -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should throw for bad IP (Too big int)" {
        { Start-Neo4jBackup -UseHost '260.1.1.1' -UsePort 6362 -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should throw if missing" {
        { Start-Neo4jBackup -UsePort 6362 -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should not throw for good IP" {
        { Start-Neo4jBackup -UseHost '10.1.2.3' -UsePort 6362 -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Not Throw
      }
    }

    Context "UsePort Parameter" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      It "should throw for bad cast (Alpha chars)" {
        { Start-Neo4jBackup -UsePort 'abcd' -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should throw for bad number (Min - 1)" {
        { Start-Neo4jBackup -UsePort 0 -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should throw for bad number (Max + 1)" {
        { Start-Neo4jBackup -UsePort 65536 -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should throw if missing" {
        { Start-Neo4jBackup -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Throw
      }
      It "should not throw for good number (Min)" {
        { Start-Neo4jBackup -UsePort 1 -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Not Throw
      }
      It "should not throw for good number (Max)" {
        { Start-Neo4jBackup -UsePort 65535 -UseHost '127.0.0.1' -To 'TestDrive:\FakeDir' -ErrorAction Stop } | Should Not Throw
      }
    }



    Context "Appends other commands" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_server'; 'Value'='1.2.3.4:1234' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter { 
         ($ArgumentList -join ' ').Contains('-someparameter somevalue')
      }
      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir' -someparameter somevalue

      It "appends additional commands" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts a new process by default" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_server'; 'Value'='1.2.3.4:1234' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable -ParameterFilter { (-not $Wait) -and (-not $NoNewWindow) }

      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir'

      It "starts a new process by default" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts shell in same process if specified" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_server'; 'Value'='1.2.3.4:1234' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable -ParameterFilter { $Wait -and $NoNewWindow }

      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir' -Wait

      It "starts a new process by default" {
        Assert-VerifiableMocks
      }
    }

    Context "Returns the Neo4jServer Object if -PassThru" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Enterprise'; }) }
      Mock Get-Neo4jSetting {@(
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_enabled'; 'Value'='true' })
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='online_backup_server'; 'Value'='1.2.3.4:1234' })
      )} 
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      $result = Start-Neo4jBackup -To 'TestDrive:\FakeDir' -PassThru

      It "returns the Neo4jServer Object if -PassThru" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
    }
  }
}



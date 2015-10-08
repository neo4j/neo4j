$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Start-Neo4jShell" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { }
      Mock Start-Process { }
      $result = Start-Neo4jShell
  
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
      $result = Start-Neo4jShell -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
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
        { Start-Neo4jShell -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }

    Context "Missing Java" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { }
      Mock Start-Process { }

      It "throws error if missing Java" {
        { Start-Neo4jShell -ErrorAction Stop } | Should Throw
      }
    }

# Raised an issue with Pester.  https://github.com/pester/Pester/issues/353.  Will enable this tests once the issue is resolved and tested.
if ($PSVersionTable.PSVersion -ne '2.0') {
    Context "Uses default values" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host localhost') -and ($ArgumentList -join ' ').Contains('-port 1337')
      }
      Start-Neo4jShell

      It "uses default values if nothing specified" {
        Assert-VerifiableMocks
      }
    }

    Context "Uses values in the configuration file" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        @(
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_enabled'; 'Value'='true' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_host'; 'Value'='somehost.domain.com' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_port'; 'Value'='1234' })
        )
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host somehost.domain.com') -and ($ArgumentList -join ' ').Contains('-port 1234')
      }
      Start-Neo4jShell

      It "uses values in the configuration file if not specified" {
        Assert-VerifiableMocks
      }
    }

    Context "Uses specified host and port" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        @(
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_enabled'; 'Value'='true' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_host'; 'Value'='somehost.domain.com' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_port'; 'Value'='1234' })
        )
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host anotherhost.domain.com') -and ($ArgumentList -join ' ').Contains('-port 5678')
      }
      
      Start-Neo4jShell -UseHost 'anotherhost.domain.com' -UsePort 5678 

      It "uses values specified in parameters" {
        Assert-VerifiableMocks
      }
    }

    Context "Uses specified port parameter alias" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        @(
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_enabled'; 'Value'='true' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_host'; 'Value'='somehost.domain.com' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_port'; 'Value'='1234' })
        )
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host anotherhost.domain.com') -and ($ArgumentList -join ' ').Contains('-port 5678')
      }
      
      Start-Neo4jShell -UseHost 'anotherhost.domain.com' -Port 5678 

      It "uses values specified in parameters" {
        Assert-VerifiableMocks
      }
    }

    Context "Uses specified shellport parameter alias" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        @(
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_enabled'; 'Value'='true' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_host'; 'Value'='somehost.domain.com' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_port'; 'Value'='1234' })
        )
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host anotherhost.domain.com') -and ($ArgumentList -join ' ').Contains('-port 5678')
      }
      
      Start-Neo4jShell -UseHost 'anotherhost.domain.com' -ShellPort 5678 

      It "uses values specified in parameters" {
        Assert-VerifiableMocks
      }
    }

    Context "Uses specified host parameter alias" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        @(
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_enabled'; 'Value'='true' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_host'; 'Value'='somehost.domain.com' })
          New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j.properties'; 'Name'='remote_shell_port'; 'Value'='1234' })
        )
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-host anotherhost.domain.com') -and ($ArgumentList -join ' ').Contains('-port 5678')
      }
      
      Start-Neo4jShell -Host 'anotherhost.domain.com' -UsePort 5678 

      It "uses values specified in parameters" {
        Assert-VerifiableMocks
      }
    }


    Context "Appends other startup commands" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-AnotherSetting settingvalue')
      }
      Start-Neo4jShell -AnotherSetting settingvalue

      It "passes additional commands to the shell" {
        Assert-VerifiableMocks
      }
    }

    Context "Appends other startup commands from java" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @('fakejava=true');}) }
      Mock Start-Process { }
      Mock Start-Process -Verifiable -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('fakejava=true')
      }
      Start-Neo4jShell

      It "appends additional java arguments" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts a new process by default" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process  { }
      Mock Start-Process -Verifiable -ParameterFilter { (-not $Wait) -and (-not $NoNewWindow) }
      Start-Neo4jShell

      It "starts a new process by default" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts shell in same process if specified" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process  { }
      Mock Start-Process -Verifiable -ParameterFilter { $Wait -and $NoNewWindow }
      Start-Neo4jShell -Wait

      It "starts shell in same process if specified" {
        Assert-VerifiableMocks
      }
    }

    Context "Returns the result code by default" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { return @{'exitcode'=255} }
      
      $result = Start-Neo4jShell

      It "returns the result code by default" {
        $result | Should Be 255
      }
    }

    Context "Returns the Neo4jServer Object if -PassThru" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting { }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { return @{'exitcode'=255} }
      
      $result = Start-Neo4jShell -PassThru

      It "returns the Neo4jServer Object if -PassThru" {
        $result.Home | Should Be 'TestDrive:\FakeDir'
      }
    }
}    

  }
}

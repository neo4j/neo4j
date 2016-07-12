$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Start-Neo4jImport" {

    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { }
      Mock Start-Process { }
      $result = Start-Neo4jImport
  
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
      $result = Start-Neo4jImport -Neo4jServer 'TestDrive:\some-dir-that-doesnt-exist'
  
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
        { Start-Neo4jImport -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
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
        { Start-Neo4jImport -ErrorAction Stop } | Should Throw
      }
    }
    
    Context "Uses default values" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='graph\db' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }

      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { return @{ 'ExitCode' = 1} } -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('--into TestDrive:\FakeDir\graph\db')
      }
      $result = Start-Neo4jImport

      It "uses default values if nothing specified" {
        Assert-VerifiableMocks
      }

      It "returns the process exit code" {
        $result | Should Be 1
      }
    }

    Context "Uses specified graph directory" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='graph\db' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }
      
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { 1 } -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('--into TestDrive:\FakeDir\graph\db')
      }
      Start-Neo4jImport --into 'TestDrive:\FakeDir\graph\db'

      It "uses --into if specified" {
        Assert-VerifiableMocks
      }
    }
    
    Context "Appends other commands" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='fakedir\throwerror' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }
      
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable { 1 } -ParameterFilter { 
        ($ArgumentList -join ' ').Contains('-someparameter somevalue')
      }
      Start-Neo4jImport -someparameter somevalue

      It "appends additional commands" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts a new process by default" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='graph\db' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable -ParameterFilter { (-not $Wait) -and (-not $NoNewWindow) }
      
      Start-Neo4jImport

      It "starts a new process by default" {
        Assert-VerifiableMocks
      }
    }

    Context "Starts shell in same process if specified" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='graph\db' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }

      Mock Start-Process -Verifiable -ParameterFilter { $Wait -and $NoNewWindow }
      Start-Neo4jImport -Wait

      It "starts shell in same process if specified" {
        Assert-VerifiableMocks
      }
    }

    Context "Returns the Neo4jServer Object if -PassThru" {
      Mock Get-Neo4jServer { return New-Object -TypeName PSCustomObject -Property (@{'Home' = 'TestDrive:\FakeDir'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'; }) }
      Mock Get-Neo4jSetting {
        New-Object -TypeName PSCustomObject -Property (@{ 'ConfigurationFile'='neo4j-server.properties'; 'Name'='org.neo4j.server.database.location'; 'Value'='graph\db' })
      } -ParameterFilter {
        $Name -eq 'org.neo4j.server.database.location'
      }
      Mock Get-Java { return New-Object -TypeName PSCustomObject -Property (@{'java'='ignoreme'; 'args' = @();}) }
      Mock Start-Process { }
      
      $result = Start-Neo4jImport -PassThru

      It "returns the Neo4jServer Object if -PassThru" {
        $result.GetType().ToString() | Should Be 'System.Management.Automation.PSCustomObject'
      }
    }
  }
}

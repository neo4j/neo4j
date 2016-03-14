$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Java" {

    # Java Detection Tests
    Context "Valid Java install in JAVA_HOME environment variable" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }
      
      Mock Test-Path -Verifiable { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be 'TestPath:\JavaHome\bin\java.exe'
      }

      It "should have empty shell arguments" {
        $result.args | Should BeNullOrEmpty
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Invalid Java install in JAVA_HOME environment variable" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }

      It "should throw if java missing" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }
    }

    Context "Valid Java install in Registry (32bit Java on 64bit OS)" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }
      
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }      
      Mock Test-Path -Verifiable { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = 'TestPath:\JavaHome'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment\9.9')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be 'TestPath:\JavaHome\bin\java.exe'
      }

      It "should have empty shell arguments" {
        $result.args | Should BeNullOrEmpty
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Invalid Java install in Registry (32bit Java on 64bit OS)" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }
      
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }      
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = 'TestPath:\JavaHome'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment\9.9')
      }
            
      It "should throw if java missing" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }   

    Context "Valid Java install in Registry" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }
      
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }      
      Mock Test-Path -Verifiable { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = 'TestPath:\JavaHome'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment\9.9')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be 'TestPath:\JavaHome\bin\java.exe'
      }

      It "should have empty shell arguments" {
        $result.args | Should BeNullOrEmpty
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Invalid Java install in Registry" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }
      
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }      
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = 'TestPath:\JavaHome'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment\9.9')
      }
            
      It "should throw if java missing" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
    
    Context "Valid Java install in search path" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }

      Mock Get-Command -Verifiable { return @{ 'Path' = 'TestPath:\JavaHome\bin\java.exe' } }
      Mock Test-Path -Verifiable { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be 'TestPath:\JavaHome\bin\java.exe'
      }

      It "should have empty shell arguments" {
        $result.args | Should BeNullOrEmpty
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "No Java install at all" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','', "Process")
      Mock Get-ItemProperty { return $null }
      Mock Get-Command { return $null }
      
      It "should throw if java not detected" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }
    }
    
    # ForServer tests
    Context "Server Invoke - Community v3.0" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '3.0';
        'ServerType' = 'Community';
        'DatabaseMode' = '';
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.CommunityEntryPoint" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.CommunityEntryPoint'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }

    Context "Server Invoke - Enterprise v3.0" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise';
        'DatabaseMode' = '';
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.enterprise.EnterpriseEntryPoint" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.enterprise.EnterpriseEntryPoint'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }

    # Utility Invoke
    Context "Utility Invoke" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe') -or
        ($Path -eq 'TestDrive:\FakeExtraClass')
      }
      Mock Get-ChildItem -ParameterFilter { $Path -eq 'TestDrive:\Path\bin' }
      Mock Get-ChildItem { @(
        @{ 'Extension'='.jar'; 'Fullname'='TestDrive:\fake1.jar'}
      )} -ParameterFilter { $Path -eq 'TestDrive:\Path\lib' }
      Mock Get-ChildItem { @(
        @{ 'Extension'='.jar'; 'Fullname'='TestDrive:\FakeExtraClass\fake2.jar'}
      )} -ParameterFilter { $Path -eq 'TestDrive:\FakeExtraClass' }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'
      })

      $result = Get-Java -ForUtility -StartingClass 'someclass' -Neo4jServer $serverObject -ErrorAction Stop
      $resultArgs = ($result.args -join ' ')

      It "should have correct ClassPath" {
        $resultArgs | Should Match ([regex]::Escape('-classpath ;"TestDrive:\fake1.jar"'))
      }
      It "should have correct Starting Class" {
        $resultArgs | Should Match ([regex]::Escape(' someclass'))
      }
    }    

    # Arbiter mode
    Context "Arbiter mode" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }

      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '3.0';
        'ServerType' = 'Enterprise'
        'DatabaseMode' = 'ARBITER'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of ArbiterBootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.enterprise.ArbiterEntryPoint'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have correct Config File" {
        $resultArgs | Should Match ([regex]::Escape('-DconfigFile="conf/neo4j-wrapper.conf"'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }
  }
}

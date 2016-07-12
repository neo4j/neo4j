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
    Context "Server Invoke - Community v2.3" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.3';
        'ServerType' = 'Community'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.CommunityBootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.CommunityBootstrapper'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }

    Context "Server Invoke - Enterprise v2.3" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.3';
        'ServerType' = 'Enterprise'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.enterprise.EnterpriseBootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.enterprise.EnterpriseBootstrapper'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }

    Context "Server Invoke - Advanced v2.3" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.3';
        'ServerType' = 'Advanced'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.advanced.AdvancedBootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.advanced.AdvancedBootstrapper'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }

    Context "Server Invoke - Community v2.2" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.2';
        'ServerType' = 'Community'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.Bootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.Bootstrapper'))
      }
    }

    Context "Server Invoke - Community v2.1" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.1';
        'ServerType' = 'Community'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.Bootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.Bootstrapper'))
      }
    }

    Context "Server Invoke - Community v2.0" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.0';
        'ServerType' = 'Community'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.Bootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.Bootstrapper'))
      }
    }


    Context "Server Invoke - Community v1.9" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '1.9';
        'ServerType' = 'Community'
      })

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.Bootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.Bootstrapper'))
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
      Mock Get-ChildItem { @(
        @{ 'Extension'='.jar'; 'Fullname'='TestDrive:\fake1.jar'}
      )} -ParameterFilter { $Path -eq 'TestDrive:\Path\lib' }
      Mock Get-ChildItem { @(
        @{ 'Extension'='.jar'; 'Fullname'='TestDrive:\FakeExtraClass\fake2.jar'}
      )} -ParameterFilter { $Path -eq 'TestDrive:\FakeExtraClass' }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community'
      })

      $result = Get-Java -ForUtility -AppName 'someapp' -StartingClass 'someclass' -ExtraClassPath 'TestDrive:\FakeExtraClass' -Neo4jServer $serverObject -ErrorAction Stop
      $resultArgs = ($result.args -join ' ')

      It "should have correct ClassPath" {
        $resultArgs | Should Match ([regex]::Escape('-classpath ;"TestDrive:\fake1.jar";"TestDrive:\FakeExtraClass\fake2.jar"'))
      }
      It "should have correct Repo" {
        $resultArgs | Should Match ([regex]::Escape('-Dapp.repo="TestDrive:\Path\lib"'))
      }
      It "should have correct BaseDir" {
        $resultArgs | Should Match ([regex]::Escape('-Dbasedir="TestDrive:\Path'))
      }
      It "should have correct App" {
        $resultArgs | Should Match ([regex]::Escape('-Dapp.name=someapp'))
      }
      It "should have correct Starting Class" {
        $resultArgs | Should Match ([regex]::Escape(' someclass'))
      }
    }    

    # Arbiter Invoke
    Context "Arbiter Invoke" {
      Mock Test-Path { $false }
      [Environment]::SetEnvironmentVariable('JAVA_HOME','TestPath:\JavaHome', "Process")
      Mock Get-ItemProperty { return $null }      
      Mock Test-Path { $true }  -ParameterFilter {
        ($Path -eq 'TestPath:\JavaHome\bin\java.exe')
      }
      
      $serverObject = (New-Object -TypeName PSCustomObject -Property @{
        'Home' = 'TestDrive:\Path';
        'ServerVersion' = '2.3';
        'ServerType' = 'Enterprise'
      })

      $result = Get-Java -ForArbiter -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.advanced.AdvancedBootstrapper" {
        $resultArgs | Should Match ([regex]::Escape('-DserverMainClass=org.neo4j.server.enterprise.StandaloneClusterClient'))
      }

      It "should have correct WorkingDir" {
        $resultArgs | Should Match ([regex]::Escape('-DworkingDir="TestDrive:\Path'))
      }

      It "should have correct Config File" {
        $resultArgs | Should Match ([regex]::Escape('-DconfigFile="conf/arbiter-wrapper.conf"'))
      }

      It "should have DserverMainClass before jar in arguments" {
        ($resultArgs.IndexOf('-DserverMainClass=') -lt $resultArgs.IndexOf(' -jar ')) | Should Be $true
      }
    }
  }
}
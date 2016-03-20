$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Java" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }

    # Java Detection Tests
    Context "Valid Java install in JAVA_HOME environment variable" {
      $result = Get-Java

      It "should return java location" {
        $result.java | Should Be "$javaHome\bin\java.exe"
      }

      It "should have empty shell arguments" {
        $result.args | Should BeNullOrEmpty
      }
    }

    Context "Invalid Java install in JAVA_HOME environment variable" {
      Mock Test-Path { $false } -ParameterFile { $Path -like "$javaHome\bin\java.exe" }
      
      It "should throw if java missing" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }
    }

    Context "Valid Java install in Registry (32bit Java on 64bit OS)" {
      Mock Get-Neo4jEnv { $null } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }      
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = $javaHome} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment\9.9')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be "$javaHome\bin\java.exe"
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid Java install in Registry" {
      Mock Get-Neo4jEnv { $null } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }      
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = $javaHome} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment\9.9')
      }
            
      $result = Get-Java

      It "should return java location from registry" {
        $result.java | Should Be "$javaHome\bin\java.exe"
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Invalid Java install in Registry" {
      Mock Test-Path { $false } -ParameterFile { $Path -like "$javaHome\bin\java.exe" }
      Mock Get-Neo4jEnv { $null } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
      Mock Test-Path -Verifiable { return $true } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }      
      Mock Get-ItemProperty -Verifiable { return @{ 'CurrentVersion' = '9.9'} } -ParameterFilter {
        ($Path -eq 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment')
      }
      Mock Get-ItemProperty -Verifiable { return @{ 'JavaHome' = $javaHome} } -ParameterFilter {
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
      Mock Get-Neo4jEnv { $null } -ParameterFilter { $Name -eq 'JAVA_HOME' } 

      Mock Get-Command -Verifiable { return @{ 'Path' = "$javaHome\bin\java.exe" } }
            
      $result = Get-Java

      It "should return java location from search path" {
        $result.java | Should Be "$javaHome\bin\java.exe"
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "No Java install at all" {
      Mock Get-Neo4jEnv { $null } -ParameterFilter { $Name -eq 'JAVA_HOME' } 
      Mock Get-Command { $null }
      
      It "should throw if java not detected" {
        { Get-Java -ErrorAction Stop } | Should Throw
      }
    }
    
    # ForServer tests
    Context "Server Invoke - Community v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Community'

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.CommunityEntryPoint" {
        $resultArgs | Should Match ([regex]::Escape(' org.neo4j.server.CommunityEntryPoint'))
      }
    }

    Context "Server Invoke - Enterprise v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Enterprise'

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.enterprise.EnterpriseEntryPoint" {
        $resultArgs | Should Match ([regex]::Escape(' org.neo4j.server.enterprise.EnterpriseEntryPoint'))
      }
    }

    Context "Server Invoke - Enterprise Arbiter v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Enterprise' -DatabaseMode 'Arbiter'

      $result = Get-Java -ForServer -Neo4jServer $serverObject
      $resultArgs = ($result.args -join ' ')

      It "should have main class of org.neo4j.server.enterprise.EnterpriseEntryPoint" {
        $resultArgs | Should Match ([regex]::Escape(' org.neo4j.server.enterprise.ArbiterEntryPoint'))
      }
    }

    # Utility Invoke
    Context "Utility Invoke" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '99.99' -ServerType 'Community'

      $result = Get-Java -ForUtility -StartingClass 'someclass' -Neo4jServer $serverObject -ErrorAction Stop
      $resultArgs = ($result.args -join ' ')

      It "should have jars from bin" {
        $resultArgs | Should Match ([regex]::Escape('\bin\bin1.jar"'))
      }
      It "should have jars from lib" {
        $resultArgs | Should Match ([regex]::Escape('\lib\lib1.jar"'))
      }
      It "should have correct Starting Class" {
        $resultArgs | Should Match ([regex]::Escape(' someclass'))
      }
    }    

  }
}

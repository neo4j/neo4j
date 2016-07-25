$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Confirm-JavaVersion" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' } 

    Context "Java returns a non zero exit code for version query" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 1} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning -Verifiable -ParameterFilter { $Message -eq 'Unable to determine Java Version' }
  
      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return true" {
        $result | Should Be $true
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Java returns a zero exit code but no content" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 0} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning -Verifiable -ParameterFilter { $Message -eq 'Unable to determine Java Version' }
  
      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return true" {
        $result | Should Be $true
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Java returns a zero exit code but invalid content" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 0} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Set-Content -Path $tempFilename -Value 'invalid java ver info' | Out-Null
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning -Verifiable -ParameterFilter { $Message -eq 'Unable to determine Java Version' }
  
      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return true" {
        $result | Should Be $true
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    # Java Detection Tests
    Context "Valid Java install (1.8 JDK) in JAVA_HOME environment variable" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 0} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Set-Content -Path $tempFilename -Value 'java version "1.8.0"`n`rJava HotSpot(TM) 64-Bit Server VM (build 11.11-a11, mixed mode)' | Out-Null
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning { }

      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return true" {
        $result | Should Be $true
      }

      It "should remove the redirection file" {
        $tempFilename | Should Not Exist
      }

      It "should not emit warnings" {
        Assert-MockCalled Write-Warning -Times 0 
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Unsupport Java install (1.8 Bad-JRE) in JAVA_HOME environment variable" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 0} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Set-Content -Path $tempFilename -Value 'java version "1.8.0"`n`rJava BadSpot(TM) 64-Bit Server VM (build 11.11-a11, mixed mode)' | Out-Null
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning -Verifiable -ParameterFilter { $Message -eq 'WARNING! You are using an unsupported Java runtime' }

      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return true" {
        $result | Should Be $true
      }

      It "should remove the redirection file" {
        $tempFilename | Should Not Exist
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Legacy Java install (1.7 JDK) in JAVA_HOME environment variable" {
      # Mock the java version output file
      Mock Start-Process -Verifiable { @{ 'exitcode' = 0} }
      $tempFilename = Join-Path -Path $TestDrive -ChildPath 'stderr.txt'
      Set-Content -Path $tempFilename -Value 'java version "1.7.0"`n`rJava HotSpot(TM) 64-Bit Server VM (build 11.11-a11, mixed mode)' | Out-Null
      Mock New-Neo4jTempFile { $tempFilename }
      Mock Write-Warning { }

      $result = Confirm-JavaVersion -Path $global:mockJavaExe

      It "should return false" {
        $result | Should Be $false
      }

      It "should remove the redirection file" {
        $tempFilename | Should Not Exist
      }

      It "should emit a warning" {
        Assert-MockCalled Write-Warning
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}

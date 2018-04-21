$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jPrunsrv" {

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
    Mock Confirm-JavaVersion { $true }
    # Mock Neo4j environment
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Set-Neo4jEnv { }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "return throw if invalid or missing neo4j directory" {
        { Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall  -ErrorAction Stop }  | Should Throw
      }
    }

    Context "Invalid or missing servicename in specified neo4j installation" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "return throw if invalid or missing service name" {
        { Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall  -ErrorAction Stop }  | Should Throw
      }
    }

    Context "Select PRUNSRV based on OS architecture" {
      $serverObject = global:New-MockNeo4jInstall
      $testCases = @(
        @{ 'AddressWidth' = 32; 'exe' = 'prunsrv-i386.exe'},
        @{ 'AddressWidth' = 64; 'exe' = 'prunsrv-amd64.exe'}
      ) | ForEach-Object -Process {
        $testCase = $_
          Mock Get-WMIObject { @{ 'AddressWidth' = $testCase.AddressWidth}}

          $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

          It "return $($testCase.exe) on $($testCase.AddressWidth)bit operating system" {
            $prunsrv.cmd  | Should Match ([regex]::Escape($testCase.exe) + '$')
          }
        }
    }

    Context "PRUNSRV arguments" {
      $serverObject = global:New-MockNeo4jInstall

      It "return //IS/xxx argument on service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//IS//$($global:mockServiceName)"))
      }

      It "return //US/xxx argument on service update" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerUpdate

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//US//$($global:mockServiceName)"))
      }

      It "return //DS/xxx argument on service uninstall" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerUninstall

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//DS//$($global:mockServiceName)"))
      }

      It "return //TS/xxx argument on service run console" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForConsole

        $prunsrv.args -join ' ' | Should Match ([regex]::Escape("//TS//$($global:mockServiceName)"))
      }
    }

    Context "PRUNSRV arguments are quoted" {
      $quotedStringRegex = ([regex]::New("^"".*""$"))
      $serverObject = global:New-MockNeo4jInstall -RootDir "TestDrive:\Neo4j Install With Space"

      It "on service install" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

        $prunsrv.args | Should Match ($quotedStringRegex)
      }

      It "on service uninstall" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerUninstall

        $prunsrv.args | Should Match ($quotedStringRegex)
      }

      It "on console" {
        $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForConsole

        $prunsrv.args | Should Match ($quotedStringRegex)
      }
    }


    Context "Server Invoke - Community v3.0" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Community'

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall

      It "should have main class of org.neo4j.server.CommunityEntryPoint" {
        ($prunsrv.args -join ' ') | Should Match ([regex]::Escape('=org.neo4j.server.CommunityEntryPoint'))
      }
    }

    Context "Server Invoke - Additional Java Parameters" {
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Community' `
        -NeoConfSettings 'dbms.logs.gc.enabled=true'

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall
      $jvmArgs = ($prunsrv.args | Where-Object { $_ -match '^\"--JvmOptions='})

      It "should specify UTF8 encoding" {
        $jvmArgs | Should Match ([regex]::Escape('-Dfile.encoding=UTF-8'))
      }

      # dbms.logs.gc.enabled=true is specified in the mock so -Xloggc:... should be present in the Prunsrv command
      It "should set GCLogfile in Prunsrv if specified in neo4j.conf" {
        $jvmArgs | Should Match ([regex]::Escape('-Xloggc:'))
      }
    }

    Context "Server Invoke - JVM Memory Settings" {
      $mockJvmMs = 130
      $mockJvmMx = 140

      # Create a mock configuration with JVM settings set
      $serverObject = global:New-MockNeo4jInstall -ServerVersion '3.0' -ServerType 'Community' `
        -NeoConfSettings "dbms.memory.heap.initial_size=$mockJvmMs","dbms.memory.heap.max_size=$mockJvmMx"

      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $serverObject -ForServerInstall
      $prunArgs = ($prunsrv.args -join ' ')

      # Reference
      # http://commons.apache.org/proper/commons-daemon/procrun.html

      It "should specify --JvmMs if dbms.memory.heap.initial_size is set" {
        $prunArgs | Should Match ([regex]::Escape("`"--JvmMs`" `"$mockJvmMs`""))
      }

      It "should specify --JvmMx if dbms.memory.heap.max_size is set" {
        $prunArgs | Should Match ([regex]::Escape("`"--JvmMx`" `"$mockJvmMx`""))
      }
    }

  }
}

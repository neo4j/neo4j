# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


<#
.SYNOPSIS
Retrieves information about PRunSrv on the local machine to start Neo4j programs

.DESCRIPTION
Retrieves information about PRunSrv (Apache Commons Daemon) on the local machine to start Neo4j services and utilites, tailored to the type of Neo4j edition

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER ForServerInstall
Retrieve the PrunSrv command line to install a Neo4j Server

.PARAMETER ForServerUninstall
Retrieve the PrunSrv command line to install a Neo4j Server

.PARAMETER ForConsole
Retrieve the PrunSrv command line to start a Neo4j Server in the console.

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
Function Get-Neo4jPrunsrv
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low',DefaultParameterSetName='ConsoleInvoke')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false)]
    [PSCustomObject]$Neo4jServer
        
    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ServerInstallInvoke')]
    [switch]$ForServerInstall

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ServerUninstallInvoke')]
    [switch]$ForServerUninstall

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ConsoleInvoke')]
    [switch]$ForConsole
  )
  
  Begin
  {
  }
  
  Process
  {
    $JavaCMD = Get-Java -Neo4jServer $Neo4jServer -ForServer -ErrorAction Stop
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return 255
    }
    
    # JVMDLL is in %JAVA_HOME%\bin\server\jvm.dll
    $JvmDLL = Join-Path -Path (Join-Path -Path (Split-Path $JavaCMD.java -Parent) -ChildPath 'server') -ChildPath 'jvm.dll'
    if (-Not (Test-Path -Path $JvmDLL)) { Throw "Could not locate JVM.DLL at $JvmDLL" }

    # Get the Service Name
    $Name = Get-Neo4jWindowsServiceName -Neo4jServer $Neo4jServer -ErrorAction Stop
    
    # Find PRUNSRV for this architecture
    # This check will return the OS architecture even when running a 32bit app on 64bit OS
    switch ( (Get-WMIObject -Class Win32_Processor | Select-Object -First 1).Addresswidth ) {
      32 { $PrunSrvName = 'prunsrv-i386.exe' }  # 4 Bytes = 32bit
      64 { $PrunSrvName = 'prunsrv-amd64.exe' } # 8 Bytes = 64bit
      default { throw "Unable to determine the architecture of this operating system (Integer is $([IntPtr]::Size))"}
    }
    $PrunsrvCMD = Join-Path (Join-Path -Path(Join-Path -Path $Neo4jServer.Home -ChildPath 'bin') -ChildPath 'tools') -ChildPath $PrunSrvName
    if (-not (Test-Path -Path $PrunsrvCMD)) { throw "Could not find PRUNSRV at $PrunsrvCMD"}

    # Build the PRUNSRV command line
    switch ($PsCmdlet.ParameterSetName) {
      "ServerInstallInvoke"   {
        $PrunArgs += @("//IS//$($Name)")

        $JvmOptions = @('-Dfile.encoding=UTF-8')
        $setting = (Get-Neo4jSetting -ConfigurationFile 'neo4j-wrapper.conf' -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
        if ($setting -ne $null) { $JvmOptions += $setting.Value }

        $PrunArgs += @('--StartMode=jvm',
          '--StartMethod=start',
          "`"--StartPath=$($Neo4jServer.Home)`"",
          "`"--StartParams=--config-dir=$($Neo4jServer.ConfDir)`"",
          '--StopMode=jvm',
          '--StopMethod=stop',
          "`"--StopPath=$($Neo4jServer.Home)`"",
          "`"--Description=Neo4j Graph Database - $($Neo4jServer.Home)`"",
          "`"--DisplayName=Neo4j Graph Database - $Name`"",
          "`"--Jvm=$($JvmDLL)`"",
          '--LogPath=logs',
          '--StdOutput=logs\neo4j.log',
          '--StdError=logs\neo4j-error.log',
          '--LogPrefix=neo4j-service',
          "`"--Classpath=$($Neo4jServer.LibDir)/*;plugins/*`"",
          "`"--JvmOptions=$($JvmOptions -join ';')`"",
          '--Startup=auto'
        )

        if ($Neo4jServer.ServerType -eq 'Enterprise') { $serverMainClass = 'org.neo4j.server.enterprise.EnterpriseEntryPoint' }
        if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityEntryPoint' }
        if ($Neo4jServer.DatabaseMode.ToUpper() -eq 'ARBITER') { $serverMainClass = 'org.neo4j.server.enterprise.ArbiterEntryPoint' }
        if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }    
        $PrunArgs += @("--StopClass=$($serverMainClass)",
                       "--StartClass=$($serverMainClass)")
      }
      "ServerUninstallInvoke" { $PrunArgs += @("//DS//$($Name)") }
      "ConsoleInvoke"         { $PrunArgs += @("//TS//$($Name)") }
      default {
        throw "Unknown ParameterSerName $($PsCmdlet.ParameterSetName)"
        return $null
      }
    }
    
    Write-Output @{'cmd' = $PrunsrvCMD; 'args' = $PrunArgs}
  }
  
  End
  {
  }
}

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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
Retrieves information about Java on the local machine to start Neo4j programs

.DESCRIPTION
Retrieves information about Java on the local machine to start Neo4j services and utilities, tailored to the type of Neo4j edition

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER ForServer
Retrieve the Java command line to start a Neo4j Server

.PARAMETER ForUtility
Retrieve the Java command line to start a Neo4j utility such as Neo4j Admin.

.PARAMETER StartingClass
The name of the starting class when invoking Java

.EXAMPLE
Get-Java -Neo4jServer $serverObject -ForServer

Retrieves the Java command line to start the Neo4j server for the instance in $serverObject.

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
function Get-Java
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low',DefaultParameterSetName = 'Default')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInvoke')]
    [pscustomobject]$Neo4jServer

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInvoke')]
    [switch]$ForServer

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [switch]$ForUtility

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [string]$StartingClass
  )

  begin
  {
  }

  process
  {
    $javaPath = ''
    $javaCMD = ''

    $EnvJavaHome = Get-Neo4jEnv 'JAVA_HOME'
    $EnvClassPrefix = Get-Neo4jEnv 'CLASSPATH_PREFIX'

    # Is JAVA specified in an environment variable
    if (($javaPath -eq '') -and ($EnvJavaHome -ne $null))
    {
      $javaPath = $EnvJavaHome
    }

    # Attempt to find Java in registry
    $regKey = 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment'
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $regJavaVersion = ''
      try
      {
        $regJavaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($regJavaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$regJavaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaPath = ''
      }
    }

    # Attempt to find Java in registry (32bit Java on 64bit OS)
    $regKey = 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment'
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $regJavaVersion = ''
      try
      {
        $regJavaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($regJavaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$regJavaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaPath = ''
      }
    }

    # Attempt to find Java in the search path
    if ($javaPath -eq '')
    {
      $javaExe = (Get-Command 'java.exe' -ErrorAction SilentlyContinue)
      if ($javaExe -ne $null)
      {
        $javaCMD = $javaExe.Path
        $javaPath = Split-Path -Path $javaCMD -Parent
      }
    }

    if ($javaPath -eq '') { Write-Error "Unable to determine the path to java.exe"; return $null }
    if ($javaCMD -eq '') { $javaCMD = "$javaPath\bin\java.exe" }
    if (-not (Test-Path -Path $javaCMD)) { Write-Error "Could not find java at $javaCMD"; return $null }

    Write-Verbose "Java detected at '$javaCMD'"

    $javaVersion = Get-JavaVersion -Path $javaCMD
    if (-not $javaVersion.isValid) { Write-Error "This instance of Java is not supported"; return $null }

    # Shell arguments for the Neo4jServer
    $ShellArgs = @()
    if ($PsCmdlet.ParameterSetName -eq 'ServerInvoke')
    {
      $serverMainClass = ''
      if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityEntryPoint' }

      if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }

      # Build the Java command line
      $ClassPath = "$($Neo4jServer.Home)/lib/*;$($Neo4jServer.Home)/plugins/*"
      $ShellArgs = @("-cp `"$($ClassPath)`"" `
          ,'-server' `
        )

      # Parse Java config settings - Heap initial size
      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.initial_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) {
        $mem = "$($option.Value)"
        if ($mem -notmatch '[\d]+[gGmMkK]') {
          $mem += "m"
          Write-Warning @"
WARNING: dbms.memory.heap.initial_size will require a unit suffix in a
         future version of Neo4j. Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.initial_size=512m
                                          ^
"@
        }
        $ShellArgs += "-Xms$mem"
      }

      # Parse Java config settings - Heap max size
      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.max_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) {
        $mem = "$($option.Value)"
        if ($mem -notmatch '[\d]+[gGmMkK]') {
          $mem += "m"
          Write-Warning @"
WARNING: dbms.memory.heap.max_size will require a unit suffix in a
         future version of Neo4j. Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.max_size=512m
                                      ^
"@
        }
        $ShellArgs += "-Xmx$mem"
      }

      # Parse Java config settings - Explicit
      $option = (Get-Neo4jSetting -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) { $ShellArgs += $option.value }

      # Parse Java config settings - GC
      $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.enabled' -Neo4jServer $Neo4jServer)
      if (($option -ne $null) -and ($option.value.ToLower() -eq 'true')) {
        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.options' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) {
          $gcOptions = $option.value
        } else {
          $gcOptions = '-Xlog:gc*,safepoint,age*=trace'
        }
        # GC file name should be escaped on Windows because of ':' usage as part of absolute name
        $gcFile = "\`"" + "$($Neo4jServer.LogDir)/gc.log" + "\`""
        $gcOptions += ":file=$( $gcFile )::"

        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.keep_number' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) {
          $gcOptions += "filecount=$( $option.value )"
        } else {
          $gcOptions += "filecount=5"
        }

        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.size' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) {
          $gcOptions += ",filesize=$( $option.value )"
        } else {
          $gcOptions += ",filesize=20m"
        }
        $ShellArgs += $gcOptions
      }
      $ShellArgs += @("-Dfile.encoding=UTF-8",
        $serverMainClass,
        "--config-dir=`"$($Neo4jServer.ConfDir)`"",
        "--home-dir=`"$($Neo4jServer.Home)`"")
    }

    # Shell arguments for the utility classes e.g. Admin
    if ($PsCmdlet.ParameterSetName -eq 'UtilityInvoke')
    {
      # Generate the commandline args
      $ClassPath = "$($Neo4jServer.Home)/lib/*;$($Neo4jServer.Home)/bin/*"

      $ShellArgs = @()
      $ShellArgs += @("-XX:+UseParallelGC",
        "-classpath `"$($EnvClassPrefix);$ClassPath`"",
        "-Dbasedir=`"$($Neo4jServer.Home)`"",`
           '-Dfile.encoding=UTF-8')

      # Determine user configured heap size.
      $HeapSize = Get-Neo4jEnv 'HEAP_SIZE'
      if ($HeapSize -ne $null) {
        $ShellArgs += "-Xmx$HeapSize"
        $ShellArgs += "-Xms$HeapSize"
      }

      # Add the starting class
      $ShellArgs += @($StartingClass)
    }
	ForEach ($arg in $Neo4jServer.AdditionalArguments)
	{
		$ShellArgs += $arg
	}
    Write-Output @{ 'java' = $javaCMD; 'args' = $ShellArgs }
  }

  end
  {
  }
}

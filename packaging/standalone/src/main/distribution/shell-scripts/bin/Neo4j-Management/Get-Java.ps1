# Copyright (c) 2002-2016 "Neo Technology,"
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
Retrieves information about Java on the local machine to start Neo4j programs

.DESCRIPTION
Retrieves information about Java on the local machine to start Neo4j services and utilites, tailored to the type of Neo4j edition

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER ForServer
Retrieve the Java command line to start a Neo4j Server

.PARAMETER ForUtility
Retrieve the Java command line to start a Neo4j utility such as Neo4j Shell.

.PARAMETER StartingClass
The name of the starting class when invoking Java

.EXAMPLE
Get-Java -Neo4jServer $serverObject -ForServer

Retrieves the Java comamnd line to start the Neo4j server for the instance in $serverObject.

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
Function Get-Java
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low',DefaultParameterSetName='Default')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ServerInvoke')]
    [PSCustomObject]$Neo4jServer
        
    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ServerInvoke')]
    [switch]$ForServer

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [switch]$ForUtility

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [string]$StartingClass    
  )
  
  Begin
  {
  }
  
  Process
  {
    $javaPath = ''
    $javaVersion = ''
    $javaCMD = ''
    
    $EnvJavaHome = Get-Neo4jEnv 'JAVA_HOME'
    $EnvClassPrefix = Get-Neo4jEnv 'CLASSPATH_PREFIX'
    
    # Is JAVA specified in an environment variable
    if (($javaPath -eq '') -and ($EnvJavaHome -ne $null))
    {
      $javaPath = $EnvJavaHome
      # Modify the java path if a JRE install is detected
      if (Test-Path -Path "$javaPath\bin\javac.exe") { $javaPath = "$javaPath\jre" }
    }

    # Attempt to find Java in registry
    $regKey = 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment'    
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $javaVersion = ''
      try
      {
        $javaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($javaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$javaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaVersion = ''
        $javaPath = ''
      }
    }

    # Attempt to find Java in registry (32bit Java on 64bit OS)
    $regKey = 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment'    
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $javaVersion = ''
      try
      {
        $javaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($javaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$javaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaVersion = ''
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

    if ($javaVersion -eq '') { Write-Verbose 'Unable to determine Java version' }
    if ($javaPath -eq '') { Write-Error "Unable to determine the path to java.exe"; return $null }
    if ($javaCMD -eq '') { $javaCMD = "$javaPath\bin\java.exe" }
    if (-not (Test-Path -Path $javaCMD)) { Write-Error "Could not find java at $javaCMD"; return $null }
 
    $ShellArgs = @()

    Write-Verbose "Java detected at '$javaCMD'"
    Write-Verbose "Java version detected as '$javaVersion'"

    # Shell arguments for the Neo4jServer and Arbiter classes
    if ($PsCmdlet.ParameterSetName -eq 'ServerInvoke')
    {
      $NeoLibDir = Join-Path -Path $Neo4jServer.Home -ChildPath $Neo4jServer.LibDir
      $serverMainClass = ''
      if ($Neo4jServer.ServerType -eq 'Enterprise') { $serverMainClass = 'org.neo4j.server.enterprise.EnterpriseEntryPoint' }
      if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityEntryPoint' }
      if ($Neo4jServer.DatabaseMode.ToUpper() -eq 'ARBITER') { $serverMainClass = 'org.neo4j.server.enterprise.ArbiterEntryPoint' }

      if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }

      # Build the Java command line
      $ClassPath="$($NeoLibDir)/*;$($Neo4jServer.Home)/plugins/*"
      $ShellArgs = @("-cp `"$($ClassPath)`""`
                    ,'-server' `
                    ,'-Dlog4j.configuration=file:conf/log4j.properties' `
                    ,'-Dneo4j.ext.udc.source=zip-powershell' `
                    ,'-Dorg.neo4j.cluster.logdirectory=data/log' `
      )

      # Parse Java config settings - Heap
      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.initial_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) { $ShellArgs += "-Xms$($option.Value)m" }

      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.max_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) { $ShellArgs += "-Xmx$($option.Value)m" }

      # Parse Java config settings - Explicit
      $option = (Get-Neo4jSetting -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) { $ShellArgs += $option.value }

      # Parse Java config settings - GC
      $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.enabled' -Neo4jServer $Neo4jServer)
      if (($option -ne $null) -and ($option.Value.ToLower() -eq 'true')) {
        $ShellArgs += "-Xloggc:$($Neo4jServer.Home)/gc.log"

        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.options' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) { $ShellArgs += @('-XX:+PrintGCDetails','-XX:+PrintGCDateStamps','-XX:+PrintGCApplicationStoppedTime','-XX:+PrintPromotionFailure','-XX:+PrintTenuringDistribution','-XX:+UseGCLogFileRotation')}

        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.size' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) {
          $ShellArgs += "-XX:GCLogFileSize=$($option.value)"
        } else {
          $ShellArgs += "-XX:GCLogFileSize=20m"
        }

        $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.keep_number' -Neo4jServer $Neo4jServer)
        if ($option -ne $null) {
          $ShellArgs += "-XX:NumberOfGCLogFiles=$($option.value)"
        } else {
          $ShellArgs += "-XX:NumberOfGCLogFiles=5"
        }
      }
      $ShellArgs += @("-Dfile.encoding=UTF-8",$serverMainClass,"--config-dir=$($Neo4jServer.ConfDir)")
    }
    
    # Shell arguments for the utility classes e.g. Import, Shell
    if ($PsCmdlet.ParameterSetName -eq 'UtilityInvoke')
    {
      $NeoLibDir = Join-Path -Path $Neo4jServer.Home -ChildPath $Neo4jServer.LibDir
      # Generate the commandline args
      $ClassPath = ''
      # Enumerate all JARS in the lib directory and add to the class path
      Get-ChildItem -Path $NeoLibDir | Where-Object { $_.Extension -eq '.jar'} | % {
        $ClassPath += "`"$($_.FullName)`";"
      }
      # Enumerate all JARS in the bin directory and add to the class path
      Get-ChildItem -Path (Join-Path  -Path $Neo4jServer.Home -ChildPath 'bin') | Where-Object { $_.Extension -eq '.jar'} | % {
        $ClassPath += "`"$($_.FullName)`";"
      }
      if ($ClassPath.Length -gt 0) { $ClassPath = $ClassPath.SubString(0, $ClassPath.Length-1) } # Strip the trailing semicolon if needed

      $ShellArgs = @()
      $ShellArgs += @("-classpath $($EnvClassPrefix);$ClassPath",
                      "-Dbasedir=`"$($Neo4jServer.Home)`"", `
                      '-Dfile.encoding=UTF-8')
            
      # Add the starting class
      $ShellArgs += @($StartingClass)
    }

    Write-Output @{'java' = $javaCMD; 'args' = $ShellArgs}
  }
  
  End
  {
  }
}

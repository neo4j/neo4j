# Copyright (c) 2002-2018 "Neo Technology,"
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
The Neo4j Server Object

.PARAMETER ExtraClassPath
Additional paths to be added the Java Class Path

.PARAMETER ForServer
Retrieve the Java command line to start a Neo4j Server

.PARAMETER ForArbiter
Retrieve the Java command line to start a Neo4j Arbiter

.PARAMETER ForUtility
Retrieve the Java command line to start a Neo4j utility such as Neo4j Shell.

.PARAMETER AppName
Application name used when invoking Java

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
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ArbiterInvoke')]
    [PSCustomObject]$Neo4jServer
        
    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ServerInvoke')]
    [switch]$ForServer

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ArbiterInvoke')]
    [switch]$ForArbiter

    ,[Parameter(Mandatory=$false,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [string[]]$ExtraClassPath = @()

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [switch]$ForUtility

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='UtilityInvoke')]
    [string]$AppName

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
    
    # Is JAVA specified in an environment variable
    if (($javaPath -eq '') -and ($Env:JAVA_HOME -ne $null))
    {
      $javaPath = $Env:JAVA_HOME
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

    # Shell arguments for the Neo4jServer and Arbiter classes
    if (($PsCmdlet.ParameterSetName -eq 'ServerInvoke') -or ($PsCmdlet.ParameterSetName -eq 'ArbiterInvoke') )
    {
      if ($PsCmdlet.ParameterSetName -eq 'ServerInvoke')
      {
        $serverMainClass = ''
        # Server Class Path for version 2.3 and above
        if ($Neo4jServer.ServerType -eq 'Advanced') { $serverMainClass = 'org.neo4j.server.advanced.AdvancedBootstrapper' }
        if ($Neo4jServer.ServerType -eq 'Enterprise') { $serverMainClass = 'org.neo4j.server.enterprise.EnterpriseBootstrapper' }
        if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityBootstrapper' }
        # Server Class Path for version 2.2 and below
        if ($Neo4jServer.ServerVersion -match '^(2\.2|2\.1|2\.0|1\.)')
        {
          $serverMainClass = 'org.neo4j.server.Bootstrapper'
        }
        if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }
        $wrapperConfig = 'neo4j-wrapper.conf'
      }
      if ($PsCmdlet.ParameterSetName -eq 'ArbiterInvoke')
      {
        $serverMainClass = 'org.neo4j.server.enterprise.StandaloneClusterClient'
        $wrapperConfig = 'arbiter-wrapper.conf'
      }
      
      # Note -DserverMainClass must appear before -jar in the argument list.  Changing this order raises a Null Pointer Exception in the Windows Service Wrapper
      $ShellArgs = @( `
        "-DworkingDir=`"$($Neo4jServer.Home)`"" `
        ,"-Djava.util.logging.config.file=`"$($Neo4jServer.Home)\conf\windows-wrapper-logging.properties`"" `
        ,"-DconfigFile=`"conf/$($wrapperConfig)`"" `
        ,"-DserverClasspath=`"lib/*.jar;system/lib/*.jar;plugins/**/*.jar;./conf*`"" `
        ,"-DserverMainClass=$($serverMainClass)" `
        ,"-jar","`"$($Neo4jServer.Home)\bin\windows-service-wrapper-5.jar`""
      )
    }
    
    # Shell arguments for the utility classes e.g. Import, Shell
    if ($PsCmdlet.ParameterSetName -eq 'UtilityInvoke')
    {
      # Get the commandline args
      $RepoPath = Join-Path  -Path $Neo4jServer.Home -ChildPath 'lib'
      #  Get the default classpath jars
      $ClassPath = ''    
      Get-ChildItem -Path $RepoPath | Where-Object { $_.Extension -eq '.jar'} | % {
        $ClassPath += "`"$($_.FullName)`";"
      }
      #  Get the additional classpath jars
      $ExtraClassPath | ForEach-Object -Process { If (Test-Path -Path $_) { Write-Output $_} } | Get-ChildItem | Where-Object { $_.Extension -eq '.jar'} | % {
        $ClassPath += "`"$($_.FullName)`";"
      }
      if ($ClassPath.Length -gt 0) { $ClassPath = $ClassPath.SubString(0, $ClassPath.Length-1) } # Strip the trailing semicolon if needed    

      $ShellArgs = @()
      if ($Env:JAVA_OPTS -ne $null) { $ShellArgs += $Env:JAVA_OPTS }
      if ($Env:EXTRA_JVM_ARGUMENTS -ne $null) { $ShellArgs += $Env:EXTRA_JVM_ARGUMENTS }
      $ShellArgs += @("-classpath $($Env:CLASSPATH_PREFIX);$ClassPath","-Dapp.repo=`"$($RepoPath)`"","-Dbasedir=`"$($Neo4jServer.Home)`"")
      
      # Add the appname and starting class
      $ShellArgs += @("-Dapp.name=$($AppName)",$StartingClass)
    }

    Write-Output @{'java' = $javaCMD; 'args' = $ShellArgs}
  }
  
  End
  {
  }
}

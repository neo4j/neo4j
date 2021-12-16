# Copyright (c) "Neo4j,"
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
Invokes various Neo4j Utilities

.DESCRIPTION
Invokes various Neo4j Utilities.  This is a generic utility function called by the external functions e.g. Admin

.PARAMETER Command
A string of the command to run.

.PARAMETER CommandArgs
Command line arguments to pass to the utility

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 4.x Neo4j Community and Enterprise Edition databases

.NOTES
This function is private to the powershell module

#>
function Invoke-Neo4jUtility
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromPipeline = $false,Position = 0)]
    [string]$Command = ''

    ,[Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [object[]]$CommandArgs = @()
  )

  begin
  {
  }

  process
  {
    # The powershell command line interpreter converts comma delimited strings into a System.Object[] array
    # Search the CommandArgs array and convert anything that's System.Object[] back to a string type
    for ($index = 0; $index -lt $CommandArgs.Length; $index++) {
      if ($CommandArgs[$index] -is [array]) {
        [string]$CommandArgs[$index] = $CommandArgs[$index] -join ','
      }
    }

    # Determine the Neo4j Home Directory.  Uses the NEO4J_HOME environment variable or a parent directory of this script
    $Neo4jHome = Get-Neo4jEnv 'NEO4J_HOME'
    if (($Neo4jHome -eq $null) -or (-not (Test-Path -Path $Neo4jHome))) {
      $Neo4jHome = Split-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -Parent
    }
    if ($Neo4jHome -eq $null) { throw "Could not determine the Neo4j home Directory.  Set the NEO4J_HOME environment variable and retry" }

    $GetJavaParams = @{}
    switch ($Command.Trim().ToLower())
    {
      "admintool" {
        Write-Verbose "Admintool command specified"
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.server.startup.Neo4jAdminCommand';
        }
        break
      }
       "server" {
        Write-Verbose "Server command specified"
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.server.startup.Neo4jCommand';
        }
        break
      }
      default {
        Write-Host "Unknown utility $Command"
        return 255
      }
    }

    # Generate the required Java invocation
    $JavaCMD = Get-Java -Neo4jHome $Neo4jHome @GetJavaParams
    if ($JavaCMD -eq $null) { throw 'Unable to locate Java' }

    $ShellArgs = $JavaCMD.args
    if ($ShellArgs -eq $null) { $ShellArgs = @() }

    # Parameters need to be wrapped in double quotes to avoid issues in case they contain spaces.
    # https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.management/start-process?view=powershell-7#parameters
    # https://github.com/PowerShell/PowerShell/issues/5576
    foreach ($CmdArg in $CommandArgs) {
      if ($CmdArg -match '^".*"$' -or $CmdArg -match "^'.*'$") {
        $ShellArgs += $CmdArg
      } else {
        $ShellArgs += "`"$CmdArg`""
      }
    }

    Write-Verbose "Starting neo4j utility using command line $($JavaCMD.java) $ShellArgs"
    $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait -NoNewWindow -Passthru -WorkingDirectory $Neo4jHome )
    return $result.exitCode
  }

  end
  {
  }
}

function Get-Java
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low',DefaultParameterSetName = 'Default')]
  param(
    [string]$Neo4jHome = '',
    [string]$StartingClass
  )

  process
  {
    $javaPath = ''
    $javaCMD = ''

    $EnvJavaHome = Get-Neo4jEnv 'JAVA_HOME'

    # Is JAVA specified in an environment variable
    if (($javaPath -eq '') -and ($EnvJavaHome -ne $null))
    {
      $javaPath = $EnvJavaHome
    }

    # Attempt to find JDK in registry
    $regKey = 'Registry::HKLM\SOFTWARE\JavaSoft\JDK'
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

    # Attempt to find JRE in registry
    $regKey = 'Registry::HKLM\SOFTWARE\JavaSoft\JRE'
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

    # Attempt to find JDK in registry (32bit JDK on 64bit OS)
    $regKey = 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\JDK'
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

    # Attempt to find JRE in registry (32bit JRE on 64bit OS)
    $regKey = 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\JRE'
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
    $ClassPath = "$($Neo4jHome)/lib/*"
    $ShellArgs = @("-cp `"$($ClassPath)`"")
	  $ShellArgs += @("-Dbasedir=`"$Neo4jHome`"")
    $ShellArgs += @($StartingClass)
    Write-Output @{ 'java' = $javaCMD; 'args' = $ShellArgs }
  }
}

function Get-JavaVersion
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false)]
    [string]$Path
  )

  process {
    $result = Invoke-ExternalCommand -Command $Path -CommandArgs @('-version')

    # Check the output
    if ($result.exitCode -ne 0) {
      Write-Warning "Unable to determine Java Version"
      Write-Host $result.capturedOutput
      return @{ 'isValid' = $true; 'isJava17' = $true }
    }

    if ($result.capturedOutput.Count -eq 0) {
      Write-Verbose "Java did not output version information"
      Write-Warning "Unable to determine Java Version"
      return @{ 'isValid' = $true; 'isJava17' = $true }
    }

    $javaHelpText = "* Please use Oracle(R) Java(TM) 17, OpenJDK(TM) 17 to run Neo4j Server.`n" +
    "* Please see https://neo4j.com/docs/ for Neo4j installation instructions."

    # Read the contents of the redirected output
    $content = $result.capturedOutput -join "`n`r"

    # Use a simple regular expression to extract the java version
    Write-Verbose "Java version response: $content"
    if ($matches -ne $null) { $matches.Clear() }
    if ($content -match 'version \"(.+)\"') {
      $javaVersion = $matches[1]
      Write-Verbose "Java Version detected as $javaVersion"
    } else {
      Write-Verbose "Could not determine the Java Version"
      Write-Warning "Unable to determine Java Version"
      return @{ 'isValid' = $true; 'isJava17' = $true }
    }

    # Check for Java Version Compatibility
    # Anything less than Java 17 will block execution
    if (($javaVersion -lt '17') -or ($javaVersion -match '^9')) {
      Write-Warning "ERROR! Neo4j cannot be started using java version $($javaVersion)"
      Write-Warning $javaHelpText
      return @{ 'isValid' = $false; 'isJava17' = $false }
    }
    # Anything less then 18 is some Java 17 version
    $isJava17 = $javaVersion -lt '18'

    # Check for Java Edition
    $regex = '(Java HotSpot\(TM\)|OpenJDK) (64-Bit Server|Server) VM'
    if (-not ($content -match $regex)) {
      Write-Warning "WARNING! You are using an unsupported Java runtime"
      Write-Warning $javaHelpText
    }

    Write-Output @{ 'isValid' = $true; 'isJava17' = $isJava17 }
  }
}

function Invoke-ExternalCommand
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,Position = 0)]
    [string]$Command = '',

    [Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [Object[]]$CommandArgs = @()
  )

  process
  {
    # Merge Command and CommandArgs into a single array that each element
    # is checked against a space and surrounded with double quoates if
    # they are already not
    $ComSpecArgs = @()
    if ($Command -match ' ' -and -not ($Command -match '\".+\"'))
    {
      $ComSpecArgs += "`"$Command`""
    }
    else
    {
      $ComSpecArgs += $Command
    }

    foreach ($Arg in $CommandArgs)
    {
      if ($Arg -match ' ' -and -not ($Arg -match '\".+\"'))
      {
        $ComSpecArgs += "`"$Arg`""
      }
      else
      {
        $ComSpecArgs += $Arg
      }
    }
    $ComSpecArgs += "2>&1"

    Write-Verbose "Invoking $ComSpecArgs"
    # cmd.exe is a bit picky about its translation of command line arguments
    # to the actual command to be executed and this is the only one that
    # found to be running both on Windows 7 and Windows 10
    # /S is required not to transform contents of $ComSpecArgs and to be used 
    # as it is.
    $Output = & $env:ComSpec /S /C """ " $ComSpecArgs " """
    Write-Verbose "Command returned with exit code $LastExitCode"

    Write-Output @{ 'exitCode' = $LastExitCode; 'capturedOutput' = $Output }
  }
}

function Get-Neo4jEnv
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,Position = 0)]
    [string]$Name
  )

  process {
    Get-ChildItem -Path Env: |
    Where-Object { $_.Name.ToUpper() -eq $Name.ToUpper() } |
    Select-Object -First 1 |
    ForEach-Object { $_.value }
  }
}

# Copyright (c) 2002-2018 "Neo4j,"
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
Invokes an external command

.DESCRIPTION
Invokes an external command using CALL operator with stderr redirected to stdout both being
captured.

.PARAMETER Command
The executable that will be invoked

.PARAMETER CommandArgs
A list of arguments that will be added to the invocation

.EXAMPLE
Invoke-ExternalCommand -Command java.exe -Args @('-version')

Start java.exe with arguments `-version` passed 

.OUTPUTS
System.Collections.Hashtable
exitCode
capturedOutput

.NOTES
This function is private to the powershell module

#>
function Invoke-ExternalCommand
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,Position = 0)]
    [string]$Command = '',

    [Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [Object[]]$CommandArgs = @()
  )

  begin
  {
  }

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

  end
  {
  }
}

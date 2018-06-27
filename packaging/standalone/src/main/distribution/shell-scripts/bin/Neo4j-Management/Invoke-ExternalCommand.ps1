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
Function Invoke-ExternalCommand
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=0)]
    [string]$Command = '',

    [parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [Object[]]$CommandArgs = @()
   )
  
  Begin
  {
  }

  Process
  {
    # This is a hack to make Windows 7 happy with quoted commands. If command is quoated but
    # does not include a space, Windows 7 produces an error about command path not found. 
    # So we need to selectively apply double quotes when command includes spaces. 
    If ($Command -match " ")
    {
        $Command = '"{0}"' -f $Command
    }
  
    Write-Verbose "Invoking $Command with arguments $($CommandArgs -join " ")"
    $Output = & "$Env:ComSpec" /C "$Command $($CommandArgs -join " ") 2>&1"
    Write-Verbose "Command returned with exit code $LastExitCode"

    Write-Output @{'exitCode' = $LastExitCode; 'capturedOutput' = $Output}
  }
  
  End
  {
  }
}

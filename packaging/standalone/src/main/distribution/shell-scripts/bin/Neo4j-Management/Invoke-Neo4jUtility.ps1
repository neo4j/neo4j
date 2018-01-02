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
Invokes various Neo4j Utilites

.DESCRIPTION
Invokes various Neo4j Utilites.  This is a generic utility function called by the external functions e.g. Shell, Import

.PARAMETER Command
A string of the command to run.

.PARAMETER CommandArgs
Command line arguments to pass to the utility

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Community and Enterprise Edition databases

.NOTES
This function is private to the powershell module

#>
Function Invoke-Neo4jUtility
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false,Position=0)]
    [string]$Command = ''

    ,[parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [object[]]$CommandArgs = @()
  )

  Begin
  {
  }

  Process
  {
    # Determine the Neo4j Home Directory.  Uses the NEO4J_HOME enironment variable or a parent directory of this script
    $Neo4jHome = Get-Neo4jEnv 'NEO4J_HOME'
    if ( ($Neo4jHome -eq $null) -or (-not (Test-Path -Path $Neo4jHome)) ) {
      $Neo4jHome = Split-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -Parent
    }
    if ($Neo4jHome -eq $null) { throw "Could not determine the Neo4j home Directory.  Set the NEO4J_HOME environment variable and retry" }
    Write-Verbose "Neo4j Root is '$Neo4jHome'"

    $thisServer = Get-Neo4jServer -Neo4jHome $Neo4jHome -ErrorAction Stop
    if ($thisServer -eq $null) { throw "Unable to determine the Neo4j Server installation information" }
    Write-Verbose "Neo4j Server Type is '$($thisServer.ServerType)'"
    Write-Verbose "Neo4j Version is '$($thisServer.ServerVersion)'"
    Write-Verbose "Neo4j Database Mode is '$($thisServer.DatabaseMode)'"

    # Check if we have administrative rights; If the current user's token contains the Administrators Group SID (S-1-5-32-544)
    if (-not [bool](([System.Security.Principal.WindowsIdentity]::GetCurrent()).groups -match "S-1-5-32-544")) {
      Write-Warning "This command does not appear to be running with administrative rights.  Some commands may fail e.g. Start/Stop"
    }

    $GetJavaParams = @{}
    switch ($Command.Trim().ToLower())
    {
      "shell" {
        Write-Verbose "Shell command specified"
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.shell.StartClient';
        }
        break
      }
      "admintool" {
        Write-Verbose "Admintool command specified"
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.commandline.admin.AdminTool';
        }
        break
      }
      "import" {
        Write-Verbose "Import command specified"
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.tooling.ImportTool';
        }
        break
      }
      "backup" {
        Write-Verbose "Backup command specified"
        if ($thisServer.ServerType -ne 'Enterprise')
        {
          throw "Neo4j Server type $($thisServer.ServerType) does not support online backup"
        }
        $GetJavaParams = @{
          StartingClass = 'org.neo4j.backup.BackupTool';
        }
        break
      }
      default {
        Write-Host "Unknown utility $Command"
        return 255
      }
    }

    # Generate the required Java invocation
    $JavaCMD = Get-Java -Neo4jServer $thisServer -ForUtility @GetJavaParams
    if ($JavaCMD -eq $null) { throw 'Unable to locate Java' }

    $ShellArgs = $JavaCMD.args
    if ($ShellArgs -eq $null) { $ShellArgs = @() }
    # Add unbounded command line arguments
    $ShellArgs += $CommandArgs

    Write-Verbose "Starting neo4j utility using command line $($JavaCMD.java) $ShellArgs"
    $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait -NoNewWindow -PassThru)
    return $result.ExitCode
  }

  End
  {
  }
}

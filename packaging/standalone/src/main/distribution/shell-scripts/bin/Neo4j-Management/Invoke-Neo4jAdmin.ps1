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
Invokes a command which manages a Neo4j Server e.g Backup, 

.DESCRIPTION
Invokes a command which manages a Neo4j Server e.g Backup, 

Invoke this function with a blank or missing command to list available commands

.PARAMETER Command
A string of the command to run.

.PARAMETER CommandArgs
Remaining command line arguments are passed to the admin tool

.EXAMPLE
Invoke-Neo4jAdmin

Prints the help text

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Community and Enterprise Edition databases

#>
Function Invoke-Neo4jAdmin
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false,Position=0)]
    [string]$Command = ''
    
    ,[parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [String[]]$CommandArgs = @()
  )
  
  Begin
  {
  }
  
  Process
  {
    try 
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

      # Convert command line args into a hash.  Format is -<name> <value>
      $ArgsHash = @{}
      For($index = 0; $index -lt $CommandArgs.Length; $index = $index + 2) {
        $key = $CommandArgs[$index]
        $value = $CommandArgs[$index + 1]
        if ($key.Substring(1) -eq '-') {
          Write-Host "Unexpected command line argument $key"
        } else {
          $key = $key.Substring(1,$key.Length - 1) # Strip the leading hyphen
          $ArgsHash."$($key)" = $value
          Write-Verbose "Found command line argument of $key"
        }
      }

      # Process the command
      switch -Regex($Command.Trim().ToLower())
      {
        "(^$|help)" {
          Write-Host (@"
Usage:

neo4j-admin import -mode <mode> -database <database-name> -from <source-directory>

    Create a new database by importing existing data.

    -mode database

      Import a database from a pre-3.0 Neo4j installation. <source-directory> is the database location (e.g.
      <neo4j-root>/data/graph.db).

neo4j-admin help

    Display this help text.
"@)
          Return 0
        }
        "import" {
          Write-Verbose "Import command specified"
          Return  [int](Invoke-Neo4jAdmin_Import -CommandArgs $ArgsHash -Neo4jServer $thisServer -ErrorAction Stop)
        }
        default {
          Write-Host "Unknown command $Command"
          Return 255
        }
      }
      # Should not get here!
      Return 2
    }
    catch {
      Write-Error $_
      Return 1
    }
  }
  
  End
  {
  }
}

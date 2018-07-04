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
Invokes a command which manages a Neo4j Server

.DESCRIPTION
Invokes a command which manages a Neo4j Server.

Invoke this function with a blank or missing command to list available commands

.PARAMETER CommandArgs
Remaining command line arguments are passed to the admin tool

.EXAMPLE
Invoke-Neo4jAdmin help

Prints the help text

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Community and Enterprise Edition databases

#>
function Invoke-Neo4jAdmin
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [Object[]]$CommandArgs = @()
  )

  begin
  {
  }

  process
  {
    # The powershell command line interpeter converts comma delimited strings into a System.Object[] array
    # Search the CommandArgs array and convert anything that's System.Object[] back to a string type
    for ($index = 0; $index -lt $CommandArgs.Length; $index++) {
      if ($CommandArgs[$index] -is [array]) {
        [string]$CommandArgs[$index] = $CommandArgs[$index] -join ','
      }
    }

    try
    {
      return [int](Invoke-Neo4jUtility -Command 'admintool' -CommandArgs $CommandArgs -ErrorAction 'Stop')
    }
    catch {
      Write-Error $_
      return 1
    }
  }

  end
  {
  }
}

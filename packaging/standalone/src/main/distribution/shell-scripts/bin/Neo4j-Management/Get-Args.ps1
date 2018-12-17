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
Processes passed in arguments array and returns filtered fields for use in other functions

.DESCRIPTION
Filters passed-in arguments array and returns whether verbose option is enabled and the rest of the arguments

.PARAMETER Arguments
An array of arguments to process

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
function Get-Args
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromPipeline = $true)]
    [array]$Arguments
  )

  begin
  {
  }

  process
  {
    if (!$Arguments) {
      $Arguments = @()
    }

    $ActualArgs = $Arguments -notmatch "^-v$|^-verbose$"
    $Verbose = $ActualArgs.Count -lt $Arguments.Count
    $ArgsAsStr = $ActualArgs -join ' '

    Write-Output @{ 'Verbose' = $Verbose; 'Args' = $ActualArgs; 'ArgsAsStr' = $ArgsAsStr }
  }

  end
  {
  }
}

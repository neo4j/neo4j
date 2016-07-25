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
Invokes Neo4j core convert tool

.DESCRIPTION
Invokes Neo4j core convert tool

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER CommandArgs
Command line arguments to pass to core convert tool

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occurred

.NOTES
This function is private to the powershell module

#>
Function Invoke-Neo4jAdmin_CoreConvert
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false,Position=0)]
    [PSCustomObject]$Neo4jServer

    ,[parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [object[]]$CommandArgs = @()
  )

  Begin
  {
  }

  Process
  {
    # Do the core conversion
    try {
      $CommandArgs += '-config'
      $CommandArgs += $($Neo4jServer.ConfDir)
      Return [int](Invoke-Neo4jUtility -Command 'Core-Convert' -CommandArgs $CommandArgs -ErrorAction 'Stop')
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

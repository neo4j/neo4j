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
Invokes Neo4j Import utility

.DESCRIPTION
Invokes Neo4j Import utility

.PARAMETER CommandArgs
Command line arguments to pass to import

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Community and Enterprise Edition databases

#>
Function Invoke-Neo4jImport
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [object[]]$CommandArgs = @()
  )
  
  Begin
  {
  }
  
  Process
  {
    # The powershell command line interpeter converts comma delimited strings into a System.Object[] array
    # Search the CommandArgs array and convert anything that's System.Object[] back to a string type
    for($index = 0; $index -lt $CommandArgs.Length; $index++) {
      if ($CommandArgs[$index].GetType().ToString() -eq 'System.Object[]') {
        [string]$CommandArgs[$index] = $CommandArgs[$index] -join ',' 
      }
    }

    try {
      Return [int](Invoke-Neo4jUtility -Command 'Import' -CommandArgs $CommandArgs -ErrorAction 'Stop')      
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

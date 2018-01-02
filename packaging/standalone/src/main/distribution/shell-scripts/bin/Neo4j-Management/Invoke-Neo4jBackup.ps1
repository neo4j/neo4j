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
Invokes Neo4j Backup utility

.DESCRIPTION
Invokes Neo4j Backup utility

.PARAMETER CommandArgs
The remaining command line arguments to pass to the Neo4j Backup

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 3.x Neo4j Enterprise Edition databases

#>
Function Invoke-Neo4jBackup
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
    try {
      Return [int](Invoke-Neo4jUtility -Command 'Backup' -CommandArgs $CommandArgs -ErrorAction 'Stop')      
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

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
Confirms a file path is a valid Neo4j installation

.DESCRIPTION
Confirms a file path is a valid Neo4j installation

.PARAMETER Neo4jHome
Full path to confirm

.EXAMPLE
'C:\Neo4j\neo4j-community' | Confirm-Neo4jHome 

Confirm the path 'C:\Neo4j\neo4j-community' is a valid Neo4j installation

.EXAMPLE
Confirm-Neo4jHome 'C:\Neo4j\neo4j-community'

Confirm the path 'C:\Neo4j\neo4j-community' is a valid Neo4j installation

.OUTPUTS
System.Boolean

.NOTES
This function is private to the powershell module

#>
Function Confirm-Neo4jHome 
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [alias('Home')]
    [string]$Neo4jHome = ''
  )
  
  Begin
  {
  }

  Process
  {
    if ( ($Neo4jHome -eq '') -or ($Neo4jHome -eq $null) ) { return $false }

    $testPath = $Neo4jHome
    if (-not (Test-Path -Path $testPath)) { return $false }

    $testPath = (Join-Path -Path $Neo4jHome -ChildPath 'system\lib')
    if (-not (Test-Path -Path $testPath)) { return $false }
    
    return $true
  }
  
  End
  {
  }
}

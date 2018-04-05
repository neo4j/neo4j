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
Returns a temporary filename with optional prefix

.DESCRIPTION
Returns a temporary filename with optional prefix

.PARAMETER Prefix
Optional prefix to for the temporary filename

.EXAMPLE
New-Neo4jTempFile

Returns a temporary filename

.OUTPUTS
String Filename of a temporary file which does not yet exist

.NOTES
This function is private to the powershell module

#>
Function New-Neo4jTempFile
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false)]
    [String]$Prefix = ''
  )

  Begin {
  }
  
  Process {
    Do { 
      $RandomFileName = Join-Path -Path ([System.IO.Path]::GetTempPath()) -ChildPath ($Prefix + [System.IO.Path]::GetRandomFileName())
    } 
    Until (-not (Test-Path -Path $RandomFileName))

    return $RandomFileName
  }
  
  End {
  }
}

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
Determines the default location of a Neo4j installation

.DESCRIPTION
Determines the default location of a Neo4j installation using the environment variable NEO4J_HOME

.EXAMPLE
Get-Neo4jHome

Returns the path to the default Neo4j installation

.LINK
http://neo4j.com/docs/stable/server-installation.html#windows-console

.OUTPUTS
System.String

#>
Function Get-Neo4jHome
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param ()
  
  Begin
  {
  }
  
  Process
  {
    $path = $Env:NEO4J_HOME
    if ( ($path -ne $null) -and (Test-Path -Path $path) ) { Write-Output $path }
  }
  
  End
  {
  }
}

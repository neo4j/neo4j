# Copyright (c) 2002-2015 "Neo Technology,"
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



Function Confirm-Neo4jServerObject
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
    [PSCustomObject]$Neo4jServer
  )
  
  Begin
  {
  }

  Process
  {
    if ($Neo4jServer -eq $null) { return $false }
    
    if ($Neo4jServer.ServerVersion -eq $null) { return $false }
    if ($Neo4jServer.Home -eq $null) { return $false }
    if ($Neo4jServer.ServerType -eq $null) { return $false }
    
    if ( ($Neo4jServer.ServerType -ne 'Community') -and ($Neo4jServer.ServerType -ne 'Enterprise') -and ($Neo4jServer.ServerType -ne 'Advanced') ) { return $false }    
    if (-not (Test-Path -Path ($Neo4jServer.Home))) { return $false }
    
    return $true
  }
  
  End
  {
  }
}

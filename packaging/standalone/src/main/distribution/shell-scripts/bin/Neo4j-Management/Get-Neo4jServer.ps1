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
Retrieves properties about a Neo4j installation

.DESCRIPTION
Retrieves properties about a Neo4j installation and outputs a Neo4j Server object.

.PARAMETER Neo4jHome
The full path to the Neo4j installation.  If an empty string is passed, the Neo4j Home is determied via Get-Neo4jHome

.EXAMPLE
Get-Neo4jServer -Neo4jHome 'C:\Neo4j'

Retrieves information about the Neo4j installation at C:\Neo4j

.EXAMPLE
'C:\Neo4j' | Get-Neo4jServer

Retrieves information about the Neo4j installation at C:\Neo4j

.EXAMPLE
Get-Neo4jServer

Retrieves information about the Neo4j installation as determined by Get-Neo4jHome

.OUTPUTS
System.Management.Automation.PSCustomObject
This is a Neo4j Server Object

.LINK
Get-Neo4jHome  

#>
Function Get-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [alias('Home')]
    [AllowEmptyString()]
    [string]$Neo4jHome = ''
  )
  
  Begin
  {
  }
  
  Process
  {
    # Get and check the Neo4j Home directory
    if ($Neo4jHome -eq '') { $Neo4jHome = Get-Neo4jHome }
    if ( ($Neo4jHome -eq '') -or ($Neo4jHome -eq $null) )
    {
      Write-Error "Could not detect the Neo4j Home directory"
      return
    }
       
    if (-not (Confirm-Neo4jHome -Neo4jHome $Neo4jHome))
    {
      Write-Error "$Neo4jHome is not a Neo4j Home directory"
      return
    }

    # Convert the path specified into an absolute path
    $Neo4jDir = Get-Item $Neo4jHome
    $Neo4jHome = $Neo4jDir.FullName.TrimEnd('\')

    # Get the information about the server
    $serverProperties = @{
      'Home' = $Neo4jHome;
      'ServerVersion' = '';
      'ServerType' = 'Community';
    }
    Get-ChildItem (Join-Path -Path $Neo4jHome -ChildPath 'system\lib') | Where-Object { $_.Name -like 'neo4j-server-*.jar' } | ForEach-Object -Process `
    {
      # if neo4j-server-enterprise-<version>.jar exists then this is the enterprise version
      if ($_.Name -like 'neo4j-server-enterprise-*.jar') { $serverProperties.ServerType = 'Enterprise' }

      # if neo4j-server-advanced-<version>.jar exists then this is the advanced version
      if ($_.Name -like 'neo4j-server-advanced-*.jar') { $serverProperties.ServerType = 'Advanced' }
      
      # Get the server version from the name of the neo4j-server-<version>.jar file
      if ($matches -ne $null) { $matches.Clear() }
      if ($_.Name -match '^neo4j-server-([\d.\-MRC]+)\.jar$') { $serverProperties.ServerVersion = $matches[1] }
    }

    $serverObject = New-Object -TypeName PSCustomObject -Property $serverProperties
    if (-not (Confirm-Neo4jServerObject -Neo4jServer $serverObject))
    {
      Write-Error "$Neo4jHome does not contain a valid Neo4j installation"
      return
    }

    Write-Output $serverObject
  }
  
  End
  {
  }
}

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
Retrieves properties about a Neo4j installation

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER ConfigurationFile
The name of the configuration file or files to parse.  If not specified the default set of all configuration files are used.  Do not use the full path, just the filename, the path is relative to '[Neo4jHome]\conf'

.PARAMETER Name
The name of the property to retrieve.  If not specified, all properties are returned.

.EXAMPLE
Get-Neo4jServer -Neo4jHome 'C:\Neo4j'

Retrieves information about the Neo4j installation at C:\Neo4j

.EXAMPLE
Get-Neo4jSetting | Format-Table

Retrieves all settings for the Neo4j installation as determined by Get-Neo4jHome and display as a table

.EXAMPLE
'C:\Neo4j' | Get-Neo4jSetting -Name 'httpport'

Retrieves all settings with the name 'httpport' from the Neo4j installation at 'C:\Neo4j'

.EXAMPLE
'C:\Neo4j' | Get-Neo4jSetting -Name 'httpport' -ConfigurationFile @('neo4j.properties','neo4j-server.properties')

Retrieves all settings with the name 'httpport' from the Neo4j installation at 'C:\Neo4j' in configuration files called 'neo4j.properties' or 'neo4j-server.properties'

.OUTPUTS
System.Management.Automation.PSCustomObject
This is a Neo4j Setting Object
Properties;
'Name' : Name of the property
'Value' : Value of the property.  Multivalue properties are string arrays (string[])
'ConfigurationFile' : Name of the configuration file where the setting is defined
'IsDefault' : Whether this setting is a default value (Reserved for future use)
'Neo4jHome' : Path to the Neo4j installation

.LINK
Get-Neo4jHome  

.LINK
Get-Neo4jServer 

#>
Function Get-Neo4jSetting
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$false)]

    [string[]]$ConfigurationFile = $null

    ,[Parameter(Mandatory=$false)]
    [string]$Name = ''
  )
  
  Begin
  {
  }

  Process
  {
    # Get the Neo4j Server information
    if ($Neo4jServer -eq $null) { $Neo4jServer = '' }
    switch ($Neo4jServer.GetType().ToString())
    {
      'System.Management.Automation.PSCustomObject'
      {
        if (-not (Confirm-Neo4jServerObject -Neo4jServer $Neo4jServer))
        {
          Write-Error "The specified Neo4j Server object is not valid"
          return
        }
        $thisServer = $Neo4jServer
      }      
      default
      {
        $thisServer = Get-Neo4jServer -Neo4jHome $Neo4jServer
      }
    }
    if ($thisServer -eq $null) { return }

    # Set the default list of configuration files    
    if ($ConfigurationFile -eq $null)
    {
      $ConfigurationFile = ('neo4j.properties','neo4j-server.properties','neo4j-wrapper.conf')
      if ($thisServer.ServerType -eq 'Enterprise') { $ConfigurationFile += 'arbiter-wrapper.conf' }
    }
   
    $ConfigurationFile | ForEach-Object -Process `
    {
      $filename = $_
      $filePath = Join-Path -Path $thisServer.Home -ChildPath "conf\$filename"
      if (Test-Path -Path $filePath)
      {
        $keyPairsFromFile = Get-KeyValuePairsFromConfFile -filename $filePath        
      }
      else
      {
        $keyPairsFromFile = $null
      }
      
      if ($keyPairsFromFile -ne $null)
      {
        $keyPairsFromFile.GetEnumerator() | Where-Object { ($Name -eq '') -or ($_.Name -eq $Name) } | ForEach-Object -Process `
        {
          $properties = @{
            'Name' = $_.Name;
            'Value' = $_.Value;
            'ConfigurationFile' = $filename;
            'IsDefault' = $false;
            'Neo4jHome' = $thisServer.Home;
          }

          Write-Output (New-Object -TypeName PSCustomObject -Property $properties)
        }
      }
    }
  }
  
  End
  {
  }
}

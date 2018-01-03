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
Remove properties of a Neo4j installation

.DESCRIPTION
Remove properties of a Neo4j installation

.PARAMETER Neo4jHome
The path to the Neo4j installation

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER ConfigurationFile
The name of the configuration file where the property is defined

.PARAMETER Name
The name of the property to remove

.PARAMETER Force
Allow the function to set the contents of the configuration file, even if the file is read-only

.EXAMPLE
'C:\Neo4j\neo4j-community' | Remove-Neo4jSetting -ConfigurationFile 'neo4j.properties' -Name 'node_auto_indexing'

Remove the node_auto_indexing property in the neo4j.properties file for the Neo4j installation at C:\Neo4j\neo4j-community

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
Get-Neo4jSetting

.LINK
Set-Neo4jSetting

#>
Function Remove-Neo4jSetting
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium',DefaultParameterSetName='ByServerObject')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true,ParameterSetName='ByServerObject')]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true,ParameterSetName='BySettingObject')]
    [alias('Home')]
    [string]$Neo4jHome

    ,[Parameter(Mandatory=$true,ParameterSetName='ByServerObject')]
    [Parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true,ParameterSetName='BySettingObject')]
    [alias('File')]
    [string]$ConfigurationFile

    ,[Parameter(Mandatory=$true,ParameterSetName='ByServerObject')]
    [Parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true,ParameterSetName='BySettingObject')]
    [alias('Setting')]
    [string]$Name

    ,[Parameter(Mandatory=$false)]
    [switch]$Force = $false
  )

  Begin
  {
  }

  Process
  {
    switch ($PsCmdlet.ParameterSetName)
    {
      "ByServerObject"
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
      }
      "BySettingObject"
      {
        $thisServer = Get-Neo4jServer -Neo4jHome $Neo4jHome
      }
      default
      {
        Write-Error "Unknown Parameterset $($PsCmdlet.ParameterSetName)"
        return
      }
    }
    if ($thisServer -eq $null) { return }

    # Check if the configuration file exists
    $filePath = Join-Path -Path $thisServer.Home -ChildPath "conf\$ConfigurationFile"
    if (Test-Path -Path $filePath)
    {
      # Find the setting
      $settingFound = $false
      $newContent = (Get-Content -Path $filePath | ForEach-Object -Process `
      {
        $originalLine = $_
        $line = $originalLine
        $misc = $line.IndexOf('#')
        if ($misc -ge 0) { $line = $line.SubString(0,$misc) }

        if ($matches -ne $null) { $matches.Clear() }
        if ($line -match "^$($Name)=(.+)`$")
        {
          $settingFound = $true
        }
        else
        {
          Write-Output $originalLine
        }
      })
      # Modify the settings file if needed
      if ($settingFound)
      {
        if ($PSCmdlet.ShouldProcess( ("Item: $($filePath) Setting: $($Name)", 'Write configuration file')))
        {
          Set-Content -Path "$filePath" -Encoding ASCII -Value $newContent -Force:$Force -Confirm:$false | Out-Null
        }
      }
    }

    $properties = @{
      'Name' = $Name;
      'Value' = $null;
      'ConfigurationFile' = $ConfigurationFile;
      'IsDefault' = $true;
      'Neo4jHome' = $thisServer.Home;
    }
    Write-Output (New-Object -TypeName PSCustomObject -Property $properties)
  }

  End
  {
  }
}

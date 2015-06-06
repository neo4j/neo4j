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



Function Set-Neo4jSetting
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

    ,[Parameter(Mandatory=$true,ParameterSetName='ByServerObject')]
    [Parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true,ParameterSetName='BySettingObject')]
    [string[]]$Value
    
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
    if ( -not (Test-Path -Path $filePath))
    {
      if ($Force)
      {
        New-Item -Path $filePath -ItemType File | Out-Null
      }
      else
      {
        Write-Error "The specified configuration file does not exist"
        return
      }
    }

    # See if the setting is already defined
    $settingChanged = $false
    $valuesSet = @()
    $valueAsText = ($Value -join "`n") + "`n" # Converting the array to string is required for PS 2.0 compatibility

    $newContent = (Get-Content -Path $filePath | ForEach-Object -Process `
    {
      $originalLine = $_
      $line = $originalLine
      $misc = $line.IndexOf('#')
      if ($misc -ge 0) { $line = $line.SubString(0,$misc) }
  
      # Get the server version from the name of the neo4j-server-<version>.jar file
      if ($matches -ne $null) { $matches.Clear() }
      if ($line -match "^$($Name)=(.+)`$")
      {
        $currentValue = $matches[1]
        if (-not $valueAsText.Contains("$($currentValue)`n"))
        {
          $originalLine = "donotwrite"
          $settingChanged = $true
        }
        else
        {
          $valuesSet += $currentValue
        }
      }
      if ($originalLine -ne "donotwrite") { Write-Output $originalLine }
    })
    $valuesSet = ($valuesSet -join '=') + '=' # Converting the array to string is required for PS 2.0 compatibility
    
    # Check if any values were not written and append if not
    $Value | ForEach-Object -Process `
    {
      if (-not $valuesSet.Contains("$($_)="))
      {
        if ($newContent -eq $null) { $newContent = @() }
        if ($newContent.GetType().ToString() -eq 'System.String') { $newContent = @($newContent) }
        $newContent += "$($Name)=$($_)"; $settingChanged = $true
      }
    }
    
    # Modify the settings file if needed
    if ($settingChanged)
    {
      if ($PSCmdlet.ShouldProcess( ("Item: $($filePath) Setting: $($Name) Value: $($Value)", 'Write configuration file')))
      {  
        Set-Content -Path "$filePath" -Encoding ASCII -Value $newContent -Force:$Force -Confirm:$false | Out-Null
      }  
    }  
    $properties = @{
      'Name' = $Name;
      'Value' = $null;
      'ConfigurationFile' = $ConfigurationFile;
      'IsDefault' = $false;
      'Neo4jHome' = $thisServer.Home;
    }
    # Cast the types back to String or String[]
    if ($Value.Count -eq 1)
    {
      $properties.Value = $Value[0]
    }
    else
    {
      $properties.Value = $Value
    }
    Write-Output (New-Object -TypeName PSCustomObject -Property $properties)
  }
  
  End
  {
  }
}

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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
The full path to the Neo4j installation.

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

.NOTES
This function is private to the powershell module

#>
function Get-Neo4jServer
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromPipeline = $true)]
    [Alias('Home')]
    [AllowEmptyString()]
    [string]$Neo4jHome = ''
  )

  begin
  {
  }

  process
  {
    # Get and check the Neo4j Home directory
    if (($Neo4jHome -eq '') -or ($Neo4jHome -eq $null))
    {
      Write-Error "Could not detect the Neo4j Home directory"
      return
    }

    if (-not (Test-Path -Path $Neo4jHome))
    {
      Write-Error "$Neo4jHome does not exist"
      return
    }

    # Convert the path specified into an absolute path
    $Neo4jDir = Get-Item $Neo4jHome
    $Neo4jHome = $Neo4jDir.FullName.TrimEnd('\')

    $ConfDir = Get-Neo4jEnv 'NEO4J_CONF'
    if ($ConfDir -eq $null)
    {
      $ConfDir = (Join-Path -Path $Neo4jHome -ChildPath 'conf')
    }

    # Get the information about the server
    $serverProperties = @{
      'Home' = $Neo4jHome;
      'ConfDir' = $ConfDir;
      'LogDir' = (Join-Path -Path $Neo4jHome -ChildPath 'logs');
      'ServerVersion' = '';
      'ServerType' = 'Community';
      'DatabaseMode' = '';
    }

    # Check if the lib dir exists
    $libPath = (Join-Path -Path $Neo4jHome -ChildPath 'lib')
    if (-not (Test-Path -Path $libPath))
    {
      Write-Error "$Neo4jHome is not a valid Neo4j installation.  Missing $libPath"
      return
    }

    # Scan the lib dir...
    Get-ChildItem (Join-Path -Path $Neo4jHome -ChildPath 'lib') | Where-Object { $_.Name -like 'neo4j-server-*.jar' } | ForEach-Object -Process `
       {
      # if neo4j-server-enterprise-<version>.jar exists then this is the enterprise version
      if ($_.Name -like 'neo4j-server-enterprise-*.jar') { $serverProperties.ServerType = 'Enterprise' }

      # Get the server version from the name of the neo4j-server-<version>.jar file
      if ($matches -ne $null) { $matches.Clear() }
      if ($_.Name -match '^neo4j-server-(\d.+)\.jar$') { $serverProperties.ServerVersion = $matches[1] }
    }
    $serverObject = New-Object -TypeName PSCustomObject -Property $serverProperties

    # Validate the object
    if ([string]$serverObject.ServerVersion -eq '') {
      Write-Error "Unable to determine the version of the installation at $Neo4jHome"
      return
    }

    # Get additional settings...
    $setting = (Get-Neo4jSetting -ConfigurationFile 'neo4j.conf' -Name 'dbms.mode' -Neo4jServer $serverObject)
    if ($setting -ne $null) { $serverObject.DatabaseMode = $setting.value }

    # Set process level environment variables
    #  These should mirror the same paths in neo4j-shared.sh
    $dirSettings = @{ 'NEO4J_DATA' = @{ 'config_var' = 'dbms.directories.data'; 'default' = (Join-Path $Neo4jHome 'data') }
      'NEO4J_LIB' = @{ 'config_var' = 'dbms.directories.lib'; 'default' = (Join-Path $Neo4jHome 'lib') }
      'NEO4J_LOGS' = @{ 'config_var' = 'dbms.directories.logs'; 'default' = (Join-Path $Neo4jHome 'logs') }
      'NEO4J_PLUGINS' = @{ 'config_var' = 'dbms.directories.plugins'; 'default' = (Join-Path $Neo4jHome 'plugins') }
      'NEO4J_RUN' = @{ 'config_var' = 'dbms.directories.run'; 'default' = (Join-Path $Neo4jHome 'run') }
    }
    foreach ($name in $dirSettings.Keys)
    {
      $definition = $dirSettings[$name]
      $configured = (Get-Neo4jSetting -ConfigurationFile 'neo4j.conf' -Name $definition['config_var'] -Neo4jServer $serverObject)
      $value = $definition['default']
      if ($configured -ne $null) { $value = $configured.value }

      if ($value -ne $null) {
        if (-not (Test-Path $value -IsValid)) {
          throw "'$value' is not a valid path entry on this system."
        }

        $absolutePathRegex = '(^\\|^/|^[A-Za-z]:)'
        if (-not ($value -match $absolutePathRegex)) {
          $value = (Join-Path -Path $Neo4jHome -ChildPath $value)
        }
      }
      Set-Neo4jEnv $name $value
    }

    # Set log dir on server object and attempt to create it if it doesn't exist
    $serverObject.LogDir = (Get-Neo4jEnv 'NEO4J_LOGS')
    if ($serverObject.LogDir -ne $null) {
      if (-not (Test-Path -PathType Container -Path $serverObject.LogDir)) {
        New-Item -ItemType Directory -Force -ErrorAction SilentlyContinue -Path $serverObject.LogDir | Out-Null
      }
    }

    #  NEO4J_CONF and NEO4J_HOME are used by the Neo4j Admin Tool
    if ((Get-Neo4jEnv 'NEO4J_CONF') -eq $null) { Set-Neo4jEnv "NEO4J_CONF" $ConfDir }
    if ((Get-Neo4jEnv 'NEO4J_HOME') -eq $null) { Set-Neo4jEnv "NEO4J_HOME" $Neo4jHome }

    # Any deprecation warnings
    $WrapperPath = Join-Path -Path $ConfDir -ChildPath 'neo4j-wrapper.conf'
    if (Test-Path -Path $WrapperPath) { Write-Warning "$WrapperPath is deprecated and support for it will be removed in a future version of Neo4j; please move all your settings to neo4j.conf" }

    Write-Output $serverObject
  }

  end
  {
  }
}

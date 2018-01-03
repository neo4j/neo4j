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
Uninstall a Neo4j Arbiter Windows Service

.DESCRIPTION
Uninstall a Neo4j Arbiter Windows Service

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER ServiceName
The name of the Neo4j Arbiter service.  If no name is specified, the name is determined from the Neo4j Configuration files (default)

.PARAMETER SucceedIfNotExist
Do not raise an error if the service does not exist

.EXAMPLE
'C:\Neo4j\neo4j-enterprise' | Uninstall-Neo4jArbiter

Uninstall the Neo4j Arbiter Windows Service for the Neo4j installation at 'C:\Neo4j\neo4j-enterprise'

.OUTPUTS
System.Management.Automation.PSCustomObject
Neo4j Server object

.NOTES
This function is only applicable to Neo4j editions which support HA

#>
Function Uninstall-Neo4jArbiter
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''
    
    ,[Parameter(Mandatory=$false)]
    [string]$ServiceName = ''

    ,[Parameter(Mandatory=$false)]
    [switch]$SucceedIfNotExist
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

    # Check if this feature is supported
    if ($thisServer.ServerType -ne 'Enterprise')
    {
      Write-Error "Neo4j Server type $($thisServer.ServerType) does not support HA"
      return
    }
    
    if ($ServiceName -eq '')
    {
      $setting = ($thisServer | Get-Neo4jSetting -ConfigurationFile 'arbiter-wrapper.conf' -Name 'wrapper.name')
      if ($setting -ne $null) { $ServiceName = $setting.Value }
    }

    if ($ServiceName -eq '')
    {
      Write-Error 'Could not find the Windows Service Name for Neo4j Arbiter'
      return
    }
    
    # Get the Service object as a WMI object.  Can only do deletions through WMI or SC.EXE
    $service = Get-WmiObject -Class Win32_Service -Filter "Name='$ServiceName'"
    if (($service -eq $null) -and (-not $SucceedIfNotExist) ) 
    {
      Write-Error "Windows service $ServiceName cannot be removed as it does not exist"
      return
    }

    if ($service -ne $null)
    {
      Stop-Service -ServiceName $ServiceName | Out-Null
      if ($PSCmdlet.ShouldProcess($ServiceName, 'Remove Windows Service'))
      {  
        $service.delete() | Out-Null
      }        
    }
    
    Write-Output $thisServer
  }
  
  End
  {
  }
}

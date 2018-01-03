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
Install a Neo4j Arbiter Windows Service

.DESCRIPTION
Install a Neo4j Arbiter Windows Service

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER Name
The name of the Neo4j Arbiter service.  If no name is specified the default of Neo4jArbiter is used

.PARAMETER DisplayName
The name of the Neo4j Arbiter service displayed in Service Manager.  If no name is specified the default of service name is used

.PARAMETER Description
The description of the Neo4j Arbiter service.  If no name is specified the default of Neo4j-HA-Arbiter is used

.PARAMETER StartType
The Start Type of the Windows Service.  Valid strings are Manual, Automatic and Disabled.  Automatic is the default

.PARAMETER SucceedIfAlreadyExists
Do not raise an error if the service already exists

.PARAMETER PassThru
Pass through the Neo4j Server object instead of the Neo4j Setting Object

.EXAMPLE
'C:\Neo4j\neo4j-enterprise' | Install-Neo4jArbiter -Name Neo4jArbiter2
Install the Neo4j Arbiter Windows Service for the Neo4j installation at 'C:\Neo4j\neo4j-enterprise', with the name Neo4jArbiter2

.EXAMPLE
'C:\Neo4j\neo4j-enterprise' | Install-Neo4jArbiter -PassThru | Start-Neo4jArbiter

Install the Neo4j Arbiter Windows Service for the Neo4j installation at 'C:\Neo4j\neo4j-enterprise' and then start the service

.OUTPUTS
System.Management.Automation.PSCustomObject
Neo4j Setting object for the service name

System.Management.Automation.PSCustomObject
Neo4j Server object (-PassThru)

.LINK
Start-Neo4jArbiter

.NOTES
This function is only applicable to Neo4j editions which support HA

#>
Function Install-Neo4jArbiter
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$false)]
    [string]$Name = 'Neo4jArbiter'

    ,[Parameter(Mandatory=$false)]
    [string]$DisplayName = ''

    ,[Parameter(Mandatory=$false)]
    [string]$Description = 'Neo4j-HA-Arbiter'

    ,[Parameter(Mandatory=$false)]
    [ValidateSet('Manual','Automatic','Disabled')]
    [string]$StartType = 'Automatic'

    ,[Parameter(Mandatory=$false)]
    [switch]$SucceedIfAlreadyExists   

    ,[Parameter(Mandatory=$false)]
    [switch]$PassThru   
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

    $JavaCMD = Get-Java -Neo4jServer $thisServer -ForArbiter
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return
    }
    
    $Name = $Name.Trim()
    if ($DisplayName -eq '') { $DisplayName = $Name }
    
    $binPath = "`"$($JavaCMD.java)`" $($JavaCMD.args -join ' ') $Name"    

    $result = $null
    if ($SucceedIfAlreadyExists)
    {
      $result = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    }

    if ($result -eq $null)
    {
      $result = (New-Service -Name $Name -Description $Description -DisplayName $DisplayName -BinaryPathName $binPath -StartupType $StartType)
    }
    
    $thisServer | Set-Neo4jSetting -ConfigurationFile 'arbiter-wrapper.conf' -Name 'wrapper.name' -Value $Name | Out-Null
        
    if ($PassThru) { Write-Output $thisServer } else { Write-Output $result }
  }
  
  End
  {
  }
}

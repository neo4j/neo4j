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



Function Remove-Neo4jArbiter
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

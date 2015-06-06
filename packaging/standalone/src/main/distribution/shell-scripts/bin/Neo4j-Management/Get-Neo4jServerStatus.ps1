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



Function Get-Neo4jServerStatus
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Medium',DefaultParameterSetName='DefaultStatus')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$false,ParameterSetName='LegacyStatus')]
    [string]$ServiceName = ''

    ,[Parameter(Mandatory=$true,ParameterSetName='LegacyStatus')]
    [Alias('Legacy')]
    [switch]$LegacyOutput
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

    # Legacy way of getting Neo4jServer Status.  VERY limited information
    if ($PsCmdlet.ParameterSetName -eq 'LegacyStatus')
    {
      # Note - This 'legacy mode' code has intentional logic errors.  It emulates the behaviour of the old Neo4jInstaller.BAT script exactly; errors and all.
      if ($ServiceName -eq '')
      {
        $setting = ($thisServer | Get-Neo4jSetting -ConfigurationFile 'neo4j-wrapper.conf' -Name 'wrapper.name')
        if ($setting -ne $null) { $ServiceName = $setting.Value }
      }
  
      if ($ServiceName -eq '')
      {
        Write-Host '"NOT INSTALLED"'
        return
      }
      
      try
      {
        $result = Get-Service -Name $ServiceName -ErrorAction 'Stop'

        switch ($result.Status)
        {
          'Running' { Write-Host '"RUNNING"' }
          'Stopped' { Write-Host '"STOPPED"' }
          default   { Write-Host '"NOT INSTALLED"' }
        }        
      }
      catch
      {
        Write-Host '"NOT INSTALLED"'
      }
    }

    if ($PsCmdlet.ParameterSetName -eq 'DefaultStatus')
    {
      Throw 'Not Implemented'
    }
  }
  
  End
  {
  }
}

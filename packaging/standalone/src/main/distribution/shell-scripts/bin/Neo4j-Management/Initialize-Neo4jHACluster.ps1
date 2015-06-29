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



Function Initialize-Neo4jHACluster
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='High')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$false)]
    [switch]$PassThru
    
    ,[Parameter(Mandatory=$true)]
    [ValidateRange(1,65535)]
    [int]$ServerID = 0

    ,[Parameter(Mandatory=$true)]
    [ValidateScript({$_ -match '^[\d\-:.]+$'})]  
    [string]$InitialHosts = ''

    ,[Parameter(Mandatory=$false)]
    [ValidateScript({$_ -match '^[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}:([\d]+|[\d]+-[\d]+)$'})]  
    [string]$ClusterServer = ''

    ,[Parameter(Mandatory=$false)]
    [ValidateScript({$_ -match '^[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}:([\d]+|[\d]+-[\d]+)$'})]  
    [string]$HAServer = ''
    
    ,[Parameter(Mandatory=$false)]
    [switch]$DisallowClusterInit
    
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
    
    if ($thisServer.ServerType -ne 'Enterprise')
    {
      Write-Error "Neo4j Server type $($thisServer.ServerType) does not support HA"
      return $null
    }

    $settings = @"
"ConfigurationFile","IsDefault","Name","Value","Neo4jHome"
"neo4j-server.properties","False","org.neo4j.server.database.mode","HA",""
"neo4j.properties","False","ha.server_id","$ServerID",""
"neo4j.properties","False","ha.initial_hosts","$InitialHosts",""
"neo4j.properties","False","ha.cluster_server","$ClusterServer",""
"neo4j.properties","False","ha.server","$HAServer",""
"neo4j.properties","False","ha.allow_init_cluster","$(-not $DisallowClusterInit)",""
"@ | ConvertFrom-CSV | ForEach-Object -Process { $_.Neo4jHome = $thisServer.Home; if ($_.Value -ne '') { Write-Output $_} } | Set-Neo4jSetting

    if ($PassThru) { Write-Output $thisServer } else { Write-Output $settings }
  }
  
  End
  {
  }
}

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
Initializes a Neo4j installation with common settings

.DESCRIPTION
Initializes a Neo4j installation with common settings such as HTTP port number.

.PARAMETER Neo4jServer
A directory path or Neo4j server object to the Neo4j instance to initialize

.PARAMETER PassThru
Pass through the Neo4j server object instead of the initialized settings

.PARAMETER HTTPPort
TCP Port used to communicate via the HTTP protocol. Valid values are 0 to 65535

.PARAMETER EnableHTTPS
Enabled the HTTPS protocol.  By default this is disable

.PARAMETER HTTPSPort
TCP Port used to communicate via the HTTPS protocol. Valid values are 0 to 65535

.PARAMETER EnableRemoteShell
Enable the Remote Shell for the Neo4j Server.  By default this is disabled

.PARAMETER RemoteShellPort
TCP Port used to communicate with the Neo4j Server. Valid values are 0 to 65535
Requires the EnableRemoteShell switch.

.PARAMETER ListenOnIPAddress
The IP Address to listen for incoming connections.  By default his is 127.0.0.1 (localhost). Valid values are IP Addresses in x.x.x.x format
Use 0.0.0.0 to use any network interface

.PARAMETER DisableAuthentication
Disable the Neo4j authentication.  By default authentication is enabled
This is only applicable to Neo4j 2.2 and above.

.PARAMETER ClearExistingDatabase
Delete the existing graph data files

.PARAMETER DisableOnlineBackup
Disable the online backup service
This only applicable to Enterprise Neo4j Servers and will raise an error on Community servers

.PARAMETER OnlineBackupServer
Host and port number to listen for online backup service requests.  This can be a single host and port, or a single host and port range
e.g. 127.0.0.1:6000 or 10.1.2.3:6000-6009
If a port range is specified, Neo4j will attempt to listen on the next free port number, starting at the lowest.
This only applicable to Enterprise Neo4j Servers and will raise an error on Community servers
 
.EXAMPLE
'C:\Neo4j\neo4j-community' | Initialize-Neo4jServer -HTTPPort 8000

Set the HTTP port to 8000 and use all other defaults for the Neo4j installation at C:\Neo4j\neo4j-community

.EXAMPLE
Get-Neo4jServer 'C:\Neo4j\neo4j-community' | Initialize-Neo4jServer -HTTPPort 8000 -EnableRemoteShell -RemoteShellPort 40000

Set the HTTP port to 8000, use the Remote Shell on port 40000 and use all other defaults for the Neo4j installation at C:\Neo4j\neo4j-community

.EXAMPLE
Initialize-Neo4jServer -Neo4jHome 'C:\Neo4j\neo4j-enterprise' -EnableHTTPS -OnlineBackupServer 127.0.0.1:5690

Enable HTTPS on the default port and the backup server on localhost port 5690 for the Neo4j installation at C:\Neo4j\neo4j-enterprise

.OUTPUTS
System.Management.Automation.PSCustomObject[]
Multiple Neo4j Setting objects

System.Management.Automation.PSCustomObject
Neo4j Server object (-PassThru)

.LINK
Get-Neo4jServer

#>
Function Initialize-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='High')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$false)]
    [switch]$PassThru
    
    ,[Parameter(Mandatory=$false)]
    [ValidateRange(0,65535)]
    [int]$HTTPPort = 7474

    ,[Parameter(Mandatory=$false)]
    [switch]$EnableHTTPS

    ,[Parameter(Mandatory=$false)]
    [ValidateRange(0,65535)]
    [int]$HTTPSPort = 7473

    ,[Parameter(Mandatory=$false)]
    [switch]$EnableRemoteShell

    ,[Parameter(Mandatory=$false)]
    [ValidateRange(0,65535)]
    [int]$RemoteShellPort = 1337

    ,[Parameter(Mandatory=$false)]
    [ValidateScript({$_ -match [IPAddress]$_ })]
    [string]$ListenOnIPAddress = '127.0.0.1'

    ,[Parameter(Mandatory=$false)]
    [switch]$DisableAuthentication
    
    ,[Parameter(Mandatory=$false)]
    [switch]$ClearExistingDatabase
    
    ,[Parameter(Mandatory=$false)]
    [switch]$DisableOnlineBackup

    ,[Parameter(Mandatory=$false)]
    [ValidateScript({$_ -match '^[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}:([\d]+|[\d]+-[\d]+)$'})]  
    [string]$OnlineBackupServer = ''
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

    if ( ($thisServer.ServerType -ne 'Enterprise') -and ($DisableOnlineBackup -or ($OnlineBackupServer -ne '') ) )
    {
      Write-Error "Neo4j Server type $($thisServer.ServerType) does not support online backup settings"
      return
    }
     
    $settings = @"
"ConfigurationFile","IsDefault","Name","Value","Neo4jHome"
"neo4j-server.properties","False","org.neo4j.server.webserver.port","$($HTTPPort)",""
"neo4j-server.properties","False","dbms.security.auth_enabled","$((-not $DisableAuthentication).ToString().ToLower())",""
"neo4j-server.properties","False","org.neo4j.server.webserver.https.enabled","$($EnableHTTPS.ToString().ToLower())",""
"neo4j-server.properties","False","org.neo4j.server.webserver.https.port","$($HTTPSPort)",""
"neo4j.properties","False","remote_shell_enabled","$($EnableRemoteShell.ToString().ToLower())",""
"neo4j.properties","False","remote_shell_port","$($RemoteShellPort)",""
"neo4j-server.properties","False","org.neo4j.server.webserver.address","$($ListenOnIPAddress)",""
"neo4j.properties","False","online_backup_enabled","$(-not $DisableOnlineBackup -and ($OnlineBackupServer -ne ''))",""
"neo4j.properties","False","online_backup_server","$($OnlineBackupServer)",""
"@ | ConvertFrom-CSV | `
      ForEach-Object -Process { $_.Neo4jHome = $thisServer.Home; if ($_.Value -ne '') { Write-Output $_} } | `
      Set-Neo4jSetting

    if ($ClearExistingDatabase)
    {
      $dbSetting = ($thisServer | Get-Neo4jSetting | ? { (($_.ConfigurationFile -eq 'neo4j-server.properties') -and ($_.Name -eq 'org.neo4j.server.database.location')) })
      $dbPath = Join-Path -Path $thisServer.Home -ChildPath $dbSetting.Value
      if (Test-Path -Path $dbPath) { Remove-Item -Path $dbPath -Recurse -Force }
    }

    if ($PassThru) { Write-Output $thisServer } else { Write-Output $settings }
  }
  
  End
  {
  }
}

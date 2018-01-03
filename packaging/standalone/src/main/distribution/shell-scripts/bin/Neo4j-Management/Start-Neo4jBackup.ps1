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
Start a Neo4j Online Backup process

.DESCRIPTION
Start a Neo4j Online Backup process

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER UseHost
The hostname of the online backup server to connect to.  If no host is specified, the host is determined from the Neo4j Configuration files (default)

.PARAMETER UsePort
The TCP port of the online backup server to connect to.  If no port is specified, the port is determined from the Neo4j Configuration files (default)

.PARAMETER To
The full path where to save the backup to

.PARAMETER Wait
Wait for the backup to complete.

.PARAMETER PassThru
Pass through the Neo4j Server object instead of the result of the backup

.PARAMETER OtherArgs
All other parameters are passed through to the Neo4j Backup Utility

.EXAMPLE
'C:\Neo4j\neo4j-enterprise' | Start-Neo4jBackup -Wait -to C:\Backup

Backup the Neo4j database to C:\Backup, and wait for the process to complete

.OUTPUTS
System.Int32
Exitcode of backup process

System.Management.Automation.PSCustomObject
Neo4j Server object (-PassThru)

.LINK
Initialize-Neo4jServer

.LINK
http://neo4j.com/docs/stable/operations-backup.html

.NOTES
This function is only applicable to Neo4j editions which support Online Backup

#>
Function Start-Neo4jBackup
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low',DefaultParameterSetName='ByDefault')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''
    
    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ByHost')]
    [Alias('Host')]
    [ValidateScript({$_ -match [IPAddress]$_ })]
    [string]$UseHost

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,ParameterSetName='ByHost')]
    [Alias('Port')]
    [ValidateRange(1,65535)]
    [int]$UsePort

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false)]
    [string]$To = ''

    ,[Parameter(Mandatory=$false)]
    [switch]$Wait

    ,[Parameter(Mandatory=$false)]
    [switch]$PassThru   
    
    ,[Parameter(ValueFromRemainingArguments = $true)]
    [Object[]]$OtherArgs
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
      Write-Error "Neo4j Server type $($thisServer.ServerType) does not support online backup"
      return
    }

    # Get the online backup settings
    $BackupEnabled = $false
    $BackupHost = '127.0.0.1:6362'
    Get-Neo4jSetting -Neo4jServer $thisServer | ForEach-Object -Process `
    {
      if (($_.ConfigurationFile -eq 'neo4j.properties') -and ($_.Name -eq 'online_backup_enabled')) { $BackupEnabled = ($_.Value.ToUpper() -eq 'TRUE') }
      if (($_.ConfigurationFile -eq 'neo4j.properties') -and ($_.Name -eq 'online_backup_server')) { $BackupHost = ($_.Value) }
    }
    if (($UseHost -ne '') -and ($UserPort -ne -1))
    {
      $BackupHost = "$($UseHost):$($UsePort)"
      $BackupEnabled = $true
    }
    if (!$BackupEnabled)
    {
      Write-Error "Online Backup Server is not enabled"
      return
    }
    
    if ($matches -ne $null) { $matches.Clear() }
    if ($BackupHost -match '^([\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}):([\d]+|[\d]+-[\d]+)$')
    {
      $serverHost = $matches[1]
      $serverport = $matches[2]
    }
    else
    {
      Write-Error "$BackupHost is an invalid Backup Server address"
      return
    }

    # Get Java
    $JavaCMD = Get-Java -Neo4jServer $thisServer -ForUtility -AppName 'neo4j-backup' -StartingClass 'org.neo4j.backup.BackupTool' -ExtraClassPath (Join-Path -Path $thisServer.Home -ChildPath 'system\coordinator\lib')
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return
    }
   
    $ShellArgs = $JavaCMD.args
    $ShellArgs += @('-host',"$serverHost")
    $ShellArgs += @('-port',"$serverport")
    $ShellArgs += @('-to',"$To")    
    # Add unbounded command line arguments
    if ($OtherArgs -ne $null) { $ShellArgs += $OtherArgs }

    $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait:$Wait -NoNewWindow:$Wait -PassThru)
    
    if ($PassThru) { Write-Output $thisServer } else { Write-Output $result.ExitCode }
  }
  
  End
  {
  }
}

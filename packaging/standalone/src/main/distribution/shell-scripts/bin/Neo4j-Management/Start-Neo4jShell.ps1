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


Function Start-Neo4jShell
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''
    
    ,[Parameter(Mandatory=$false,ValueFromPipeline=$false)]
    [string]$UseHost = ''

    ,[Parameter(Mandatory=$false,ValueFromPipeline=$false)]
    [Alias('ShellPort')]
    [ValidateRange(0,65535)]
    [int]$UsePort = -1
    
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

    $ShellRemoteEnabled = $false
    $ShellHost = '127.0.0.1'
    $Port = 1337    
    Get-Neo4jSetting -Neo4jServer $thisServer | ForEach-Object -Process `
    {
      if (($_.ConfigurationFile -eq 'neo4j.properties') -and ($_.Name -eq 'remote_shell_enabled')) { $ShellRemoteEnabled = ($_.Value.ToUpper() -eq 'TRUE') }
      if (($_.ConfigurationFile -eq 'neo4j.properties') -and ($_.Name -eq 'remote_shell_host')) { $ShellHost = ($_.Value) }
      if (($_.ConfigurationFile -eq 'neo4j.properties') -and ($_.Name -eq 'remote_shell_port')) { $Port = [int]($_.Value) }
    }
    if (!$ShellRemoteEnabled) { $ShellHost = 'localhost' }
    if ($UseHost -ne '') { $ShellHost = $UseHost }
    if ($UsePort -ne -1) { $Port = $UsePort }

    $JavaCMD = Get-Java -Neo4jServer $thisServer -ForUtility -AppName 'neo4j-shell' -StartingClass 'org.neo4j.shell.StartClient'
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return
    }
    
    $ShellArgs = $JavaCMD.args
    if ($ShellArgs -eq $null) { $ShellArgs = @() }
    $ShellArgs += @('-host',"$ShellHost")
    $ShellArgs += @('-port',"$Port")
    # Add unbounded command line arguments
    if ($OtherArgs -ne $null) { $ShellArgs += $OtherArgs }

    $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait:$Wait -NoNewWindow:$Wait -PassThru)
    
    if ($PassThru) { Write-Output $thisServer } else { Write-Output $result.ExitCode }
  }
  
  End
  {
  }
}

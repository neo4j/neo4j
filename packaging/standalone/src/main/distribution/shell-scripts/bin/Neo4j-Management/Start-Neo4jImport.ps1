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
Start a Neo4j Import process

.DESCRIPTION
Start a Neo4j Import process

.PARAMETER Neo4jServer
An object representing a Neo4j Server.  Either an empty string (path determined by Get-Neo4jHome), a string (path to Neo4j installation) or a valid Neo4j Server object

.PARAMETER FromPipeline
Specifies that the Neo4jServer information should be read from the pipeline

.PARAMETER Wait
Wait for the import process to complete

.PARAMETER PassThru
Pass through the Neo4j Server object instead of the result of the import

.PARAMETER OtherArgs
All other parameters are passed through to the Neo4j Import Utility

.EXAMPLE
Start-Neo4jImport -Neo4jServer 'C:\Neo4j\neo4j-community' -Wait --nodes "C:\movies.csv" --nodes "C:\actors.csv" --relationships "C:\roles.csv"

Import the nodes (movies.csv and actors.csv) and relationships (roles.csv) into the Neo4j installation at C:\Neo4j\neo4j-community and wait for the process to complete

.OUTPUTS
System.Int32
Exitcode of import process

System.Management.Automation.PSCustomObject
Neo4j Server object (-PassThru)

.LINK
Initialize-Neo4jServer

.LINK
http://neo4j.com/docs/stable/import-tool.html

#>
Function Start-Neo4jImport
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Medium',DefaultParameterSetName='Default')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true,ParameterSetName='Default')]
    [Parameter(Mandatory=$true,ValueFromPipeline=$true,ParameterSetName='FromPipeline')]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$true,ParameterSetName='FromPipeline')]
    [switch]$FromPipeline
    
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


    # Get Java
    $JavaCMD = Get-Java -Neo4jServer $thisServer -ForUtility -AppName 'neo4j-import' -StartingClass 'org.neo4j.tooling.ImportTool' -ExtraClassPath (Join-Path -Path $thisServer.Home -ChildPath 'system\coordinator\lib')
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return
    }
    
    # Get the path to the graph data directory
    $graphPath = ''
    $setting = ($thisServer | Get-Neo4jSetting -ConfigurationFile 'neo4j-server.properties' -Name 'org.neo4j.server.database.location')
    if ($setting -ne $null) { $graphPath = "$($thisServer.Home)\$($setting.Value.Replace('/','\'))" }

    $ShellArgs = $JavaCMD.args
    # Add unbounded command line arguments.  Check if --into was specified
    $intoParam = $false
    if ($OtherArgs -ne $null)
    {
      $ShellArgs += $OtherArgs
      $OtherArgs | ForEach-Object -Process `
      {
        $intoParam = $intoParam -or ($_.ToUpper() -eq '--INTO')
      }
    }
    # Insert the --into param if it's not already specified
    if (-not $intoParam)
    {
      if ([string]$graphPath -eq '')
      {
        Write-Error 'Unable to determine the graph directory from the configuration settings'
        return  
      }

      $ShellArgs += @('--into',$graphPath)
    }

    $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait:$Wait -NoNewWindow:$Wait -PassThru)
    
    if ($PassThru) { Write-Output $thisServer } else { Write-Output $result.ExitCode }
  }
  
  End
  {
  }
}

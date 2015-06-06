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



Function Start-Neo4jArbiter
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium',DefaultParameterSetName='WindowsService')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$true)]
    [object]$Neo4jServer = ''

    ,[Parameter(Mandatory=$true,ParameterSetName='Console')]
    [switch]$Console

    ,[Parameter(Mandatory=$false)]
    [switch]$Wait

    ,[Parameter(Mandatory=$false)]
    [switch]$PassThru   
    
    ,[Parameter(Mandatory=$false,ParameterSetName='WindowsService')]
    [string]$ServiceName = ''
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
    
    $JavaCMD = Get-Java -BaseDir $thisServer.Home
    if ($JavaCMD -eq $null)
    {
      Throw "Unable to locate Java"
      return
    }

    if ($PsCmdlet.ParameterSetName -eq 'Console')
    {    
  
      $ShellArgs = @( `
        "-DworkingDir=`"$($thisServer.Home)`"" `
        ,"-Djava.util.logging.config.file=`"$($thisServer.Home)\conf\windows-wrapper-logging.properties`"" `
        ,"-DconfigFile=`"conf/arbiter-wrapper.conf`"" `
        ,"-DserverClasspath=`"lib/*.jar;system/lib/*.jar;plugins/**/*.jar;./conf*`"" `
        ,"-DserverMainClass=org.neo4j.server.enterprise.StandaloneClusterClient" `
        ,"-jar","$($thisServer.Home)\bin\windows-service-wrapper-5.jar"      
      )
      $result = (Start-Process -FilePath $JavaCMD.java -ArgumentList $ShellArgs -Wait:$Wait -NoNewWindow:$Wait -PassThru -WorkingDirectory $thisServer.Home )
      
      if ($PassThru) { Write-Output $thisServer } else { Write-Output $result.ExitCode }
    }
    
    if ($PsCmdlet.ParameterSetName -eq 'WindowsService')
    {
      if ($ServiceName -eq '')
      {
        $setting = ($thisServer | Get-Neo4jSetting -ConfigurationFile 'arbiter-wrapper.conf' -Name 'wrapper.name')
        if ($setting -ne $null) { $ServiceName = $setting.Value }
      }

      if ($ServiceName -eq '')
      {
        Throw "Could not find the Windows Service Name for Neo4j Arbiter"
        return
      }

      $result = Start-Service -Name $ServiceName -PassThru
      if ($PassThru) { Write-Output $thisServer } else { Write-Output $result }
    }
  }
  
  End
  {
  }
}

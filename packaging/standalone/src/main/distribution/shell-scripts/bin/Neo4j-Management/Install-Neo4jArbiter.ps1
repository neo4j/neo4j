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
    
    # Optionally run the service under an account other than LOCAL SYSTEM?
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
    
    if ($DisplayName -eq '') { $DisplayName = $Name }

    $binPath = "`"$($JavaCMD.java)`"" + `
               " -DworkingDir=`"$($thisServer.Home)`"" + `
               " -Djava.util.logging.config.file=`"$($thisServer.Home)\conf\windows-wrapper-logging.properties`"" + `
               " -DconfigFile=`"conf/arbiter-wrapper.conf`"" + `
               " -Dorg.neo4j.cluster.logdirectory==`"$($thisServer.Home)\data\log`"" + `
               " -DserverClasspath=`"lib/*.jar;system/lib/*.jar;plugins/**/*.jar;./conf*`"" + `
               " -DserverMainClass=org.neo4j.server.enterprise.StandaloneClusterClient" + `
               " -jar `"$($thisServer.Home)\bin\windows-service-wrapper-5.jar`"" + `
               " $Name"

    $result = $null
    if ($SucceedIfAlreadyExists)
    {
      $result = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    }
    if ($result -eq $null) { $result = (New-Service -Name $Name -Description $Description -DisplayName $Name -BinaryPathName $binPath -StartupType $StartType) }
    
    $thisServer | Set-Neo4jSetting -ConfigurationFile 'arbiter-wrapper.conf' -Name 'wrapper.name' -Value $Name | Out-Null
    
    
    if ($PassThru) { Write-Output $thisServer } else { Write-Output $result }
  }
  
  End
  {
  }
}

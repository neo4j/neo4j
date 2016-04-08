# Copyright (c) 2002-2016 "Neo Technology,"
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
Uninstall a Neo4j Server Windows Service

.DESCRIPTION
Uninstall a Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.EXAMPLE
Uninstall-Neo4jServer -Neo4jServer $ServerObject

Uninstall the Neo4j Windows Windows Service for the Neo4j installation at $ServerObject

.OUTPUTS
System.Int32
0 = Service is uninstalled or did not exist
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Uninstall-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$true,ConfirmImpact='Medium')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
    [PSCustomObject]$Neo4jServer
  )
  
  Begin
  {
  }

  Process
  {
    $Name = Get-Neo4jWindowsServiceName -Neo4jServer $Neo4jServer -ErrorAction Stop

    $service = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    if ($service -eq $null) 
    {
      Write-Verbose "Windows Service $Name does not exist"
      Write-Host "Neo4j uninstalled"
      return 0
    }

    if ($service.State -ne 'Stopped') {
      Write-Host "Stopping the Neo4j service"
      Stop-Service -ServiceName $Name -ErrorAction 'Stop' | Out-Null
    }

    $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $Neo4jServer -ForServerUninstall
    if ($prunsrv -eq $null) { throw "Could not determine the command line for PRUNSRV" }

    Write-Verbose "Uninstalling Neo4j as a service with command line $($prunsrv.cmd) $($prunsrv.args)"
    $result = (Start-Process -FilePath $prunsrv.cmd -ArgumentList $prunsrv.args -Wait -NoNewWindow -PassThru -WorkingDirectory $Neo4jServer.Home)
    Write-Verbose "Returned exit code $($result.ExitCode)"

    Write-Output $result.ExitCode
    
    Write-Host "Neo4j service uninstalled"
  }
  
  End
  {
  }
}

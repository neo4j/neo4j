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
Install a Neo4j Server Windows Service

.DESCRIPTION
Install a Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.EXAMPLE
Install-Neo4jServer -Neo4jServer $ServerObject

Install the Neo4j Windows Windows Service for the Neo4j installation at $ServerObject

.OUTPUTS
System.Int32
0 = Service is installed or already exists
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Install-Neo4jServer
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Medium')]
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

    $result = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    if ($result -eq $null)
    {
      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $Neo4jServer -ForServerInstall
      if ($prunsrv -eq $null) { throw "Could not determine the command line for PRUNSRV" }

      Write-Verbose "Installing Neo4j as a service with command line $($prunsrv.cmd) $($prunsrv.args)"
      $stdError = New-Neo4jTempFile -Prefix 'stderr'
      $result = (Start-Process -FilePath $prunsrv.cmd -ArgumentList $prunsrv.args -Wait -NoNewWindow -PassThru -WorkingDirectory $Neo4jServer.Home -RedirectStandardError $stdError)
      Write-Verbose "Returned exit code $($result.ExitCode)"

      # Process the output
      if ($result.ExitCode -eq 0) {
        Write-Host "Neo4j service installed"
      } else {
        Write-Host "Neo4j service did not install"
        # Write out STDERR if it did not install
        Get-Content -Path $stdError -ErrorAction 'SilentlyContinue' | ForEach-Object -Process {
          Write-Host $_
        }
      }

      # Remove the temp file
      If (Test-Path -Path $stdError) { Remove-Item -Path $stdError -Force | Out-Null }

      Write-Output $result.ExitCode
    } else {
      Write-Verbose "Service already installed"
      Write-Output 0
    }    
  }
  
  End
  {
  }
}

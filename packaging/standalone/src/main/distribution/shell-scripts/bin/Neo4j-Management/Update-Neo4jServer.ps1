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
Update an installed Neo4j Server Windows Service

.DESCRIPTION
Update an installed Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server

.EXAMPLE
Update-Neo4jServer $ServerObject

Update the Neo4j Windows Service for the Neo4j installation at $ServerObject

.OUTPUTS
System.Int32
0 = Service is successfully updated
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Update-Neo4jServer
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
    if ($result -ne $null)
    {
      $prunsrv = Get-Neo4jPrunsrv -Neo4jServer $Neo4jServer -ForServerUpdate
      if ($prunsrv -eq $null) { throw "Could not determine the command line for PRUNSRV" }

      Write-Verbose "Update installed Neo4j service with command line $($prunsrv.cmd) $($prunsrv.args)"
      $stdError = New-Neo4jTempFile -Prefix 'stderr'
      Write-Verbose $prunsrv
      $result = (Start-Process -FilePath $prunsrv.cmd -ArgumentList $prunsrv.args -Wait -NoNewWindow -PassThru -WorkingDirectory $Neo4jServer.Home -RedirectStandardError $stdError)
      Write-Verbose "Returned exit code $($result.ExitCode)"

      # Process the output
      if ($result.ExitCode -eq 0) {
        Write-Host "Neo4j service updated"
      } else {
        Write-Host "Neo4j service did not update"
        # Write out STDERR if it did not update
        Get-Content -Path $stdError -ErrorAction 'SilentlyContinue' | ForEach-Object -Process {
          Write-Host $_
        }
      }

      # Remove the temp file
      If (Test-Path -Path $stdError) { Remove-Item -Path $stdError -Force | Out-Null }

      Write-Output $result.ExitCode
    } else {
      Write-Host "Service update failed - service '$Name' not found"
      Write-Output 1
    }
  }

  End
  {
  }
}


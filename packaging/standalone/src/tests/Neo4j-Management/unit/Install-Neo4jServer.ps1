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


<#
.SYNOPSIS
Install a Neo4j Server Windows Service

.DESCRIPTION
Install a Neo4j Server Windows Service

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.EXAMPLE
'C:\Neo4j' | Get-Neo4jServer | Install-Neo4jServer

Install the Neo4j Windows Windows Service for the Neo4j installation at 'C:\Neo4j\neo4j-enterprise'

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
    $thisServer = $Neo4jServer

    # Get the Java information
    $JavaCMD = $null
    try {
      $JavaCMD = Get-Java -Neo4jServer $thisServer -ForServer -ErrorAction Stop
    }
    catch {
      $JavaCMD = $null
    }
    if ($JavaCMD -eq $null)
    {
      Write-Error 'Unable to locate Java'
      return 255
    }

    $Name = Get-Neo4jWindowsServiceName -Neo4jServer $thisServer -ErrorAction Stop
    $DisplayName = "Neo4j Graph Database - $Name"
    $Description = "Neo4j Graph Database - $($thisServer.Home)"
    
    $binPath = "`"$($JavaCMD.java)`" $($JavaCMD.args -join ' ') $Name"    

    $result = $null
    # Check if it already exists
    $result = Get-Service -Name $Name -ComputerName '.' -ErrorAction 'SilentlyContinue'
    if ($result -eq $null)
    {
      Write-Verbose "Installing the Windows Service $Name with Bin Path $binPath"
      $result = (New-Service -Name $Name -Description $Description -DisplayName $DisplayName -BinaryPathName $binPath -StartupType 'Automatic')
    } else {
      Write-Verbose "Service already installed"
    }
    
    Write-Host "Neo4j service installed"
    return 0
  }
  
  End
  {
  }
}

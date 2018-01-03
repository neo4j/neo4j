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
Invokes Neo4j import tool

.DESCRIPTION
Invokes Neo4j import tool

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER CommandArgs
An hashtable of command line args from the command line.

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
This function is private to the powershell module

#>
Function Invoke-Neo4jAdmin_Import
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false,Position=0)]
    [PSCustomObject]$Neo4jServer
    
    ,[parameter(Mandatory=$true)]
    [Hashtable]$CommandArgs
  )
  
  Begin
  {
  }
  
  Process
  {
    
    # Check that required parameters are set
    $AllOk = $true
    'database','from','mode' | ForEach-Object {
       if (-not $CommandArgs.ContainsKey($_)) {
         Write-Host "You must provide the $_ command line argument"
         $AllOk = $false
       }
    }
    if (-not $AllOk) { return 1 }
    
    # Process the Import
    switch ($CommandArgs.mode) {
      "database" {
        Write-Verbose "Verifying source path exists"
        if (-not (Test-Path -Path $CommandArgs.From)) {
          Write-Host "Source path '$($CommandArgs.From)' does not exist"
          return 1
        }
        
        $dest = Join-Path (Join-Path -Path (Join-Path -Path $($Neo4jServer.Home) -ChildPath 'data') -ChildPath 'databases') -ChildPath $CommandArgs.Database
        Write-Verbose "Destination directory is $dest"
        
        if (-not (Test-Path -Path $dest)) {
          Write-Verbose "Creating destination directory as it does not exist"
          New-Item -Path $dest -ItemType Directory -ErrorAction Stop | Out-Null
        }
        
        Write-Verbose "Copying from $($CommandArgs.From) to $dest ..."
        Copy-Item -Path "$($CommandArgs.From)\*" -Destination $dest -Recurse -Force -ErrorAction Stop | Out-Null
        
        Write-Verbose "Removing previous messages.log if it exists"
        $messagesLog = Join-Path -Path $dest -ChildPath 'messages.log'
        If (Test-Path -Path $messagesLog) { Remove-Item -Path $messagesLog -Force | Out-Null}
        
        Write-Host "Imported data from $($CommandArgs.From) to $dest"
        
        return 0
      }
      default {
        Write-Host "Unrecognized mode '$($CommandArgs.Mode)'"
        return 1
      }
    }
  }
  
  End
  {
  }
}

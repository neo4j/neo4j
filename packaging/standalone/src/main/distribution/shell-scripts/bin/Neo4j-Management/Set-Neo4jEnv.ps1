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
Sets a process-level environment variable value

.DESCRIPTION
Sets a process-level environment variable value.  This is a helper function which aids testing and mocking

.PARAMETER Name
Name of the environment variable

.PARAMETER Value
Value of the environment variable

.EXAMPLE
Set-Neo4jEnv 'Neo4jHome' 'C:\neo4j'

Sets the Neo4jHome environment variable to C:\neo4j

.OUTPUTS
System.String
Value of the environment variable

.NOTES
This function is private to the powershell module

#>
Function Set-Neo4jEnv
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=0)]
    [String]$Name

    ,[Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=1)]
    [String]$Value
  )

  Begin
  {
  }

  Process {
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
  }

  End
  {
  }
}

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
Writes a string to STDERR

.DESCRIPTION
Writes a string to STDERR.  Will use an appropriate function call depending on the Powershell Host.

.PARAMETER Message
A string to write

.EXAMPLE
Write-StdErr "This is a message"

Outputs the string onto STDERR

.NOTES
This function is private to the powershell module

#>
Function Write-StdErr($Message) {
  if ($Host.Name -eq 'ConsoleHost') { 
    [Console]::Error.WriteLine($Message)
  } else {
    $host.UI.WriteErrorLine($Message)
  }
}

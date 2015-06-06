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



Function Get-KeyValuePairsFromConfFile
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false)]
    [string]$Filename
  )

 Process
 {
    $properties = @{}
    Get-Content -Path $filename -Filter $Filter | ForEach-Object -Process `
    {
      $line = $_
      $misc = $line.IndexOf('#')
      if ($misc -ge 0) { $line = $line.SubString(0,$misc) }
  
      if ($matches -ne $null) { $matches.Clear() }
      if ($line -match '^([^=]+)=(.+)$')
      {
        $keyName = $matches[1].Trim()
        if ($properties.Contains($keyName))
        {
          # There is already a property with this name so it must by a collection of properties.  Turn the value into an array and add it
          if (($properties."$keyName").GetType().ToString() -eq 'System.String') { $properties."$keyName" = [string[]]@($properties."$keyName") }
          $properties."$keyName" = $properties."$keyName" + $matches[2].Trim()
        }
        else
        {
          $properties."$keyName" = $matches[2].Trim()
        }        
      }
    }
    Write-Output $properties
  }
}

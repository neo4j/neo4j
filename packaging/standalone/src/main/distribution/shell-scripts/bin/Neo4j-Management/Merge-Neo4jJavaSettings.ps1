# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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
Merges two sets of JVM Settings together

.DESCRIPTION
Merges two sets of JVM Settings together

.PARAMETER Source
A string array of the original settings

.PARAMETER Additional
A string array of the settings to merge into the Source

.OUTPUTS
System.String[]

.NOTES
This function is private to the powershell module

#>
function Merge-Neo4jJavaSettings
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low',DefaultParameterSetName = 'Default')]
  param(
    [Parameter(Mandatory = $true)]
    [AllowEmptyCollection()]
    [array]$Source

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInstallInvoke')]
    [AllowEmptyCollection()]
    [array]$Additional
  )

  $SettingNameRegEx = '^(?:-D|-XX:[+-]?)([^=]+)(?:$|=.+$)'

  # Populate the initial hashtable with extracted setting names
  $SettingOutput = @{}
  $Source | ForEach-Object -Process {
    if ($matches -ne $null) { $matches.Clear() }
    if ($_ -match $SettingNameRegEx) {
      $SettingOutput.Add($_,$matches[1])
    } else {
      $SettingOutput.Add($_,'')
    }
  }

  $Additional | ForEach-Object -Process {
    $thisSetting = $_
    if ($matches -ne $null) { $matches.Clear() }
    if ($thisSetting -match $SettingNameRegEx) {
      $thisSettingName = $matches[1]

      $oldValue = $null
      $SettingOutput.GetEnumerator() | ForEach-Object -Process {
        if ($_.value -eq $thisSettingName) { $oldValue = $_.Key }
      }

      if ($oldValue -eq $null) {
        Write-Verbose "Adding '$thisSetting'"
        $SettingOutput.Add($thisSetting,'')
      } else {
        Write-Verbose "Merging '$thisSetting'"
        $SettingOutput.Remove($oldValue)
        $SettingOutput.Add($thisSetting,$thisSettingName)
      }
    } else {
      Write-Verbose "Adding '$thisSetting'"
      $SettingOutput.Add($thisSetting,'')
    }
  }

  # Java Options have an order which is important.
  #  Move the enabling of experimental features to the beginning
  if ($SettingOutput.ContainsKey('-XX:+UnlockExperimentalVMOptions')) {
    Write-Verbose "Moving -XX:+UnlockExperimentalVMOptions to the beginning"
    Write-Output '-XX:+UnlockExperimentalVMOptions'
    $SettingOutput.Remove('-XX:+UnlockExperimentalVMOptions') | Out-Null
  }

  $SettingOutput.GetEnumerator() | ForEach-Object -Process { Write-Output $_.Key.ToString() }
}

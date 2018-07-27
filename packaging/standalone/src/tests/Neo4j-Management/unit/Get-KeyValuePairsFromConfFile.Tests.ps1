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

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.",".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
.$common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-KeyValuePairsFromConfFile" {
    Context "Invalid Filename path" {
      It "throw if Filename parameter not set" {
        { Get-KeyValuePairsFromConfFile -ErrorAction Stop } | Should Throw
      }
      It "throw if Filename doesn't exist" {
        { Get-KeyValuePairsFromConfFile -FileName 'TestDrive:\some-file-that-doesnt-exist' -ErrorAction Stop } | Should Throw
      }
      It "throw if Filename parameter is the wrong type" {
        { Get-KeyValuePairsFromConfFile -FileName ('TestDrive:\some-file-that-doesnt-exist','TestDrive:\another-file-that-doesnt-exist') -ErrorAction Stop } | Should Throw
      }
    }

    Context "Valid Filename path" {
      $mockFile = 'TestDrive:\MockKVFile.txt'

      It "should ignore hash characters" {
        "setting1=value1`n`r#setting3=value3" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 1
      }
      It "simple regex test" {
        "setting1=value1" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1'
      }
      It "single entries are strings" {
        "setting1=value1" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1'
        $result.setting1.GetType().ToString() | Should Be 'System.String'
      }
      It "duplicate entries are arrays" {
        "setting1=value1`n`rsetting1=value2`n`rsetting1=value3`n`rsetting1=value4`n`rsetting1=value5" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile
        $result.Count | Should Be 1
        $result.setting1.GetType().ToString() | Should Be 'System.Object[]'
        $result.setting1.Count | Should Be 5
      }
      It "complex regex test" {
        "setting1=value1`n`rsetting2`n`r=value2" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1'
      }
      It "ignore whitespace" {
        "setting1 =value1`n`rsetting2= value2`n`r setting3 = value3 `n`rsetting4=val ue4" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 4
        $result.setting1 | Should Be 'value1'
        $result.setting2 | Should Be 'value2'
        $result.setting3 | Should Be 'value3'
        $result.setting4 | Should Be 'val ue4'
      }

      It "return empty hashtable if empty file" {
        "" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false

        $result = Get-KeyValuePairsFromConfFile -FileName $mockFile

        $result.Count | Should Be 0
      }
    }
  }
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Merge-Neo4jJavaSettings" {

    Context "Empty Source and Additional inputs" {
      $result = [array](Merge-Neo4jJavaSettings -Source @() -Additional @())
      
      It "returns empty array" {
        $result.Count | Should Be 0
      }
    }

    Context "Empty Source input" {
      $result = [array](Merge-Neo4jJavaSettings -Source @() -Additional @('-Dkey1=value1'))
      
      It "returns Additional input array" {
        $result[0] | Should Be '-Dkey1=value1'
      }
    }

    Context "Empty Additional input" {
      $result = [array](Merge-Neo4jJavaSettings -Source @('-Dkey1=value1') -Additional @())

      It "returns Source input array" {
        $result[0] | Should Be '-Dkey1=value1'
      }
    }

    Context "Ignores duplicates" {
      $result = [array](Merge-Neo4jJavaSettings -Source @('-Dkey1=value1','-Dkey2=value2') -Additional @('-Dkey1=value1'))
      
      $sorted = $result | Sort-Object -Descending:$false
      It "should ignore duplicates" {
        $result.Count | Should Be 2
        $sorted[0] | Should Be '-Dkey1=value1'
        $sorted[1] | Should Be '-Dkey2=value2'
      }
    }

    Context "Adds new settings" {
      $result = [array](Merge-Neo4jJavaSettings -Source @('-Dkey1=value1','-Dkey2=value2') -Additional @('-Dkey3=value3'))
      
      $sorted = $result | Sort-Object -Descending:$false
      It "should add new settings" {
        $result.Count | Should Be 3

        $sorted[0] | Should Be '-Dkey1=value1'
        $sorted[1] | Should Be '-Dkey2=value2'
        $sorted[2] | Should Be '-Dkey3=value3'
      }
    }

    Context "Merges settings" {
      $result = [array](Merge-Neo4jJavaSettings `
        -Source @('-Dkey9=value9','-Dkey1=value1','-XX:key2=value2') `
        -Additional @('-Dkey1=valuez','-XX:key2=valuez'))
      
      $sorted = $result | Sort-Object -Descending:$false
      It "should merge conflicting settings" {
        $result.Count | Should Be 3
      }

      It "should merge conflicting setting values" {
        $sorted[0] | Should Be '-Dkey1=valuez'
        $sorted[1] | Should Be '-Dkey9=value9'
        $sorted[2] | Should Be '-XX:key2=valuez'
      }
    }

    Context "Appends non-standard JVM settings" {
      # Settings that are in non-standard format (i.e. not -D or -XX) are just appended
      $result = [array](Merge-Neo4jJavaSettings `
        -Source @('badsetting=1') `
        -Additional @('badsetting=2'))
      
      $sorted = $result | Sort-Object -Descending:$false
      It "should append non-standard settings" {
        $result.Count | Should Be 2

        $sorted[0] | Should Be 'badsetting=1'
        $sorted[1] | Should Be 'badsetting=2'
      }
    }
  }
}

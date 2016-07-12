$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-KeyValuePairsFromConfFile" {
    Context "Invalid Filename path" {
      It "throw if Filename parameter not set" {
        { Get-KeyValuePairsFromConfFile -ErrorAction Stop } | Should Throw
      }
      It "throw if Filename doesn't exist" {
        { Get-KeyValuePairsFromConfFile -Filename 'TestDrive:\some-file-that-doesnt-exist' -ErrorAction Stop } | Should Throw
      }
      It "throw if Filename parameter is the wrong type" {
        { Get-KeyValuePairsFromConfFile -Filename ('TestDrive:\some-file-that-doesnt-exist','TestDrive:\another-file-that-doesnt-exist') -ErrorAction Stop } | Should Throw
      }
    }
  
    Context "Valid Filename path" {
      $mockFile = 'TestDrive:\MockKVFile.txt'
  
      It "should ignore hash characters" {
        "setting1=value1`n`r#setting3=value3" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 1
      }
      It "simple regex test" {
        "setting1=value1" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1' 
      }
      It "single entries are strings" {
        "setting1=value1" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1' 
        $result.setting1.GetType().ToString() | Should Be 'System.String' 
      }
      It "duplicate entries are arrays" {
        "setting1=value1`n`rsetting1=value2`n`rsetting1=value3`n`rsetting1=value4`n`rsetting1=value5" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        $result.Count | Should Be 1
        $result.setting1.GetType().ToString() | Should Be 'System.Object[]' 
        $result.setting1.Count | Should Be 5
      }
      It "complex regex test" {
        "setting1=value1`n`rsetting2`n`r=value2" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 1
        $result.setting1 | Should Be 'value1' 
      }
      It "ignore whitespace" {
        "setting1 =value1`n`rsetting2= value2`n`r setting3 = value3 `n`rsetting4=val ue4" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 4
        $result.setting1 | Should Be 'value1'
        $result.setting2 | Should Be 'value2'
        $result.setting3 | Should Be 'value3'
        $result.setting4 | Should Be 'val ue4'
      }
  
      It "return empty hashtable if empty file" {
        "" | Out-File -FilePath $mockFile -Encoding ASCII -Force -Confirm:$false            
        
        $result = Get-KeyValuePairsFromConfFile -Filename $mockFile
        
        $result.Count | Should Be 0
      }
    }
  }
}
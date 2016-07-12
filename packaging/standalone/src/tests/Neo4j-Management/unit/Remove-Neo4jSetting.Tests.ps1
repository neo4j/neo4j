$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Remove-Neo4jSetting" {
    Context "Invalid or missing default neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Remove-Neo4jSetting -Name 'invalid-setting' -ConfigurationFile 'invalid-configurationfile'
      
      It "return null if missing default" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing specified neo4j installation" {
      Mock Get-Neo4jServer { return }
      $result = Remove-Neo4jSetting -Name 'invalid-setting' -ConfigurationFile 'invalid-configurationfile' -Neo4jHome 'TestDrive:\some-dir-that-doesnt-exist'
  
      It "return null if invalid directory" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
  
    Context "Invalid or missing server object" {
      Mock Confirm-Neo4jServerObject { return $false }
      
      It "throws error for an invalid server object" {
        { Remove-Neo4jSetting -Name 'invalid-setting' -ConfigurationFile 'invalid-configurationfile' -Neo4jServer (New-Object -TypeName PSCustomObject) -ErrorAction Stop } | Should Throw
      }
  
      It "calls Confirm-Neo4jServerObject" {
        Assert-MockCalled Confirm-Neo4jServerObject -Times 1
      }
    }
  
    Context "Invalid specified neo4j installation in setting object" {
      Mock Get-Neo4jServer { return $null } -ParameterFilter { $Neo4jHome -eq 'TestDrive:\some-dir-that-doesnt-exist' }
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\some-dir-that-doesnt-exist'; 'ConfigurationFile' = 'invalid-configurationfile'; 'Name' = 'invalid-setting'; 'Value' = 'invalid-value'; 'IsDefault' = $false }
      $result = ($setting | Remove-Neo4jSetting)
  
      It "return null if invalid directory" {
        $result | Should BeNullOrEmpty      
      }
      It "calls Get-Neo4Server" {
        Assert-MockCalled Get-Neo4jServer -Times 1
      }
    }
    
    Context "Invalid configuration file" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\some-dir-that-doesnt-exist'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }    
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\some-dir-that-doesnt-exist'; 'ConfigurationFile' = 'invalid-configurationfile'; 'Name' = 'invalid-setting'; 'Value' = ''; }
      $result = ($setting | Remove-Neo4jSetting)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
    }
  
    Context "Valid configuration file - single setting value to remove" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return 'setting1=value1' } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = ($setting | Remove-Neo4jSetting -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "removed the value from the file" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid configuration file - other settings remain" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return ('setting1=value1','setting2=value2') } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1') -and ($Value -contains 'setting2=value2')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = ($setting | Remove-Neo4jSetting -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "other settings remain in the file" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid configuration file - multiple setting values to remove" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return ('setting1=value1','setting1=value2') } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1') -and ($Value -notcontains 'setting1=value2')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = ($setting | Remove-Neo4jSetting -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "removed the value from the file" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid configuration file with -WhatIf" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return 'setting1=value1' } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = ($setting | Remove-Neo4jSetting -WhatIf)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "file is not written to" {
        Assert-MockCalled Set-Content -Exactly 0
      }
    }    

    Context "Valid configuration file using the Home alias" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return 'setting1=value1' } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = (Remove-Neo4jSetting -Home ($setting.Neo4jHome) -ConfigurationFile ($setting.ConfigurationFile) -Name ($setting.Name) -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "removed the value from the file" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid configuration file using the File alias" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return 'setting1=value1' } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = (Remove-Neo4jSetting -Neo4jHome ($setting.Neo4jHome) -File ($setting.ConfigurationFile) -Name ($setting.Name) -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "removed the value from the file" {
        Assert-VerifiableMocks
      }
    }

    Context "Valid configuration file using the Setting alias" {
      Mock Get-Neo4jServer { return $serverObject = New-Object -TypeName PSCustomObject -Property @{ 'Home' = 'TestDrive:\Path'; 'ServerVersion' = '99.99'; 'ServerType' = 'Community';} }
      Mock Test-Path { $true } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Get-Content { return 'setting1=value1' } -ParameterFilter { $Path -eq 'TestDrive:\Path\conf\neo4j.properties'}
      Mock Set-Content { }
      Mock Set-Content -Verifiable { } -ParameterFilter {
         ($Path -eq 'TestDrive:\Path\conf\neo4j.properties') -and ($Value -notcontains 'setting1=value1')
      }   
      
      $setting = New-Object -TypeName PSCustomObject -Property @{ 'Neo4jHome' = 'TestDrive:\Path'; 'ConfigurationFile' = 'neo4j.properties'; 'Name' = 'setting1'; 'Value' = 'value1'; }
      $result = (Remove-Neo4jSetting -Neo4jHome ($setting.Neo4jHome) -ConfigurationFile ($setting.ConfigurationFile) -Setting ($setting.Name) -Confirm:$false)
      
      It "returns the name" {
        ($result.Name -eq $setting.Name) | Should Be $true
      }
      It "returns the configuration file" {
        ($result.ConfigurationFile -eq $setting.ConfigurationFile) | Should Be $true
      }
      It "returns the Neo4jHome" {
        ($result.Neo4jHome -eq $setting.Neo4jHome) | Should Be $true
      }     
      It "returns null value" {
        $result.Value | Should BeNullorEmpty
      }
      It "returns default value" {
        $result.IsDefault | Should Be $true
      }
      It "removed the value from the file" {
        Assert-VerifiableMocks
      }
    }
  }
}
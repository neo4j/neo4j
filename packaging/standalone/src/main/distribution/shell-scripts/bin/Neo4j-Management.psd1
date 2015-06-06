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
#
# Module manifest for module 'Neo4j-Management'
#


@{

# Script module or binary module file associated with this manifest
# RootModule = ''

# Version number of this module.
ModuleVersion = '2.3.0'

# ID used to uniquely identify this module
GUID = 'dd9cad34-ad03-439b-b347-590625302c38'

# Author of this module
Author = 'Network Engine for Objects'

# Company or vendor of this module
CompanyName = 'Network Engine for Objects'

# Copyright statement for this module
Copyright = 'http://neo4j.com/licensing/'

# Description of the functionality provided by this module
# Description = ''

# Minimum version of the Windows PowerShell engine required by this module
PowerShellVersion = '2.0'

# Name of the Windows PowerShell host required by this module
# PowerShellHostName = ''

# Minimum version of the Windows PowerShell host required by this module
# PowerShellHostVersion = ''

# Minimum version of the .NET Framework required by this module
# DotNetFrameworkVersion = ''

# Minimum version of the common language runtime (CLR) required by this module
# CLRVersion = ''

# Processor architecture (None, X86, Amd64) required by this module
# ProcessorArchitecture = ''

# Modules that must be imported into the global environment prior to importing this module
# RequiredModules = @()

# Assemblies that must be loaded prior to importing this module
# RequiredAssemblies = @()

# Script files (.ps1) that are run in the caller's environment prior to importing this module
# ScriptsToProcess = @()

# Type files (.ps1xml) to be loaded when importing this module
# TypesToProcess = @()

# Format files (.ps1xml) to be loaded when importing this module
# FormatsToProcess = @()

# Modules to import as nested modules of the module specified in RootModule/ModuleToProcess
NestedModules = @('Neo4j-Management\Neo4j-Management.psm1')

# Functions to export from this module
FunctionsToExport = @(
'Get-Neo4jHome'
'Get-Neo4jServer'
'Get-Neo4jServerStatus'
'Get-Neo4jSetting'
'Initialize-Neo4jHACluster'
'Initialize-Neo4jServer'
'Install-Neo4jArbiter'
'Install-Neo4jServer'
'Remove-Neo4jSetting'
'Restart-Neo4jArbiter'
'Restart-Neo4jServer'
'Set-Neo4jSetting'
'Start-Neo4jArbiter'
'Start-Neo4jBackup'
'Start-Neo4jImport'
'Start-Neo4jServer'
'Start-Neo4jShell'
'Stop-Neo4jArbiter'
'Stop-Neo4jServer'
'Uninstall-Neo4jArbiter'
'Uninstall-Neo4jServer'
)

# Cmdlets to export from this module
CmdletsToExport = ''

# Variables to export from this module
VariablesToExport = ''

# Aliases to export from this module
AliasesToExport = ''

# List of all modules packaged with this module
# ModuleList = @()

# List of all files packaged with this module
# FileList = @()

# Private data to pass to the module specified in RootModule/ModuleToProcess
# PrivateData = ''

# HelpInfo URI of this module - Powershell 3.0 and above only
# HelpInfoURI = 'http://neo4j.com/xxxx'

# Default prefix for commands exported from this module. Override the default prefix using Import-Module -Prefix.
# DefaultCommandPrefix = ''

}

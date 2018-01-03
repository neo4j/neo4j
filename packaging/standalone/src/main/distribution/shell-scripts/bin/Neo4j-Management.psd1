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
#
# Module manifest for module 'Neo4j-Management'
#


@{
ModuleVersion = '2.3.0'

GUID = 'dd9cad34-ad03-439b-b347-590625302c38'

Author = 'Network Engine for Objects'

CompanyName = 'Network Engine for Objects'

Copyright = 'http://neo4j.com/licensing/'

Description = 'Powershell module to manage a Neo4j instance on Windows'

PowerShellVersion = '2.0'

NestedModules = @('Neo4j-Management\Neo4j-Management.psm1')

FunctionsToExport = @(
'Get-Neo4jHome'
'Get-Neo4jServer'
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

CmdletsToExport = ''

VariablesToExport = ''

AliasesToExport = ''
}

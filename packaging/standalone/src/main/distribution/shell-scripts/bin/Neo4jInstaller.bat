@ECHO OFF
rem Copyright (c) 2002-2015 "Neo Technology,"
rem Network Engine for Objects in Lund AB [http://neotechnology.com]
rem
rem This file is part of Neo4j.
rem
rem Neo4j is free software: you can redistribute it and/or modify
rem it under the terms of the GNU General Public License as published by
rem the Free Software Foundation, either version 3 of the License, or
rem (at your option) any later version.
rem
rem This program is distributed in the hope that it will be useful,
rem but WITHOUT ANY WARRANTY; without even the implied warranty of
rem MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem GNU General Public License for more details.
rem
rem You should have received a copy of the GNU General Public License
rem along with this program.  If not, see <http://www.gnu.org/licenses/>.

SETLOCAL ENABLEEXTENSIONS

IF NOT [%2]==[] SET serviceName=%2
IF NOT [%3]==[] SET serviceDisplayName=%3

IF NOT [%serviceName%]==[] set serviceName=-Name '%serviceName%'
IF NOT [%serviceDisplayName%]==[] set serviceDisplayName=-DisplayName '%serviceDisplayName%'

IF [%1] == []        GOTO :Usage
IF [%1] == [remove]  GOTO :Remove
IF [%1] == [install] GOTO :Install
IF [%1] == [stop]    GOTO :Stop
IF [%1] == [start]   GOTO :Start
IF [%1] == [status]  GOTO :Status
CALL :Usage
EXIT /B 0

:Usage
  ECHO This script is provided for legacy purposes.  Please use the Powershell Module
  ECHO Usage: %~dp0Neo4jInstaller.bat ^<install^|remove^|stop^|start^|status^> [service name] [service display name]
  ECHO        - Service Name - Optional, must NOT contain spaces
  ECHO        - Service Display Name - Optional, The name displayed in the services window, surround with quotes to use spaces
  EXIT /B 0
  
:Status
  Powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Get-Neo4jServer '%~dp0..' | Get-Neo4jServerStatus %serviceName% -Legacy)"
  EXIT /B %ERRORLEVEL%

:Stop
  ECHO This script is provided for legacy purposes.  Please use the Powershell Module
  Powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Get-Neo4jServer '%~dp0..' | Stop-Neo4jServer %serviceName%  -Legacy)"
  EXIT /B %ERRORLEVEL%

:Start
  ECHO This script is provided for legacy purposes.  Please use the Powershell Module
  Powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Get-Neo4jServer '%~dp0..' | Start-Neo4jServer %serviceName% -Legacy)"
  EXIT /B %ERRORLEVEL%

:Remove
  ECHO This script is provided for legacy purposes.  Please use the Powershell Module
  Powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Get-Neo4jServer '%~dp0..' | Uninstall-Neo4jServer %serviceName% -Legacy)"
  EXIT /B %ERRORLEVEL%

:Install
  ECHO This script is provided for legacy purposes.  Please use the Powershell Module
  Powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Get-Neo4jServer '%~dp0..' | Install-Neo4jServer %serviceName% %serviceDisplayName% -Legacy -PassThru | Start-Neo4jServer -Legacy)"
  EXIT /B %ERRORLEVEL%

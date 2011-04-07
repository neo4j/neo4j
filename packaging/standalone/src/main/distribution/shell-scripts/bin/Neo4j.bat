rem Copyright (c) 2002-2011 "Neo Technology,"
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

 
@echo off

rem This script is the main controller for the server
rem There are commands to install, uninstall, start, stop 
rem and restart the service on windows and also run it as
rem a console process. For the first four operations however,
rem Administrator rights are required.
rem
rem To do that, you can go to
rem
rem Start -> All Programs -> Accessories -> Right click on Command Prompt
rem Click "Run as Administrator"
rem
rem Provide confirmation and/or the Administrator password if requested.
rem From the command prompt that will come up, navigate to the directory
rem containing the unpacked distribution and issue the command you wish,
rem for example
rem
rem bin\Neo4j.bat install
rem
rem to install Neo4j as a Windows Service.

setlocal
call "%~dp0"setenv.bat

set _WRAPPER_CONF_DEFAULT="..\conf\neo4j-wrapper.conf"
set _WRAPPER_CONF=%_WRAPPER_CONF_DEFAULT%

set _REALPATH=%~dp0

set _WRAPPER_EXE="%_REALPATH%wrapper.bat"

for /F %%v in ('echo %1^|findstr "^help$ ^console$ ^start$ ^stop$ ^query$ ^restart$ ^install$ ^remove$"') do call :exec set COMMAND=%%v

if "%COMMAND%" == "" (
    set COMMAND=console
)

goto %COMMAND%
if errorlevel 1 goto callerror
goto :eof

:callerror
echo An error occurred in the process.
pause
goto :eof

:help
echo Usage: %0 { console : start : stop : restart : install : remove }
pause
goto :eof

:console
call %wrapper_bat% -c %_WRAPPER_CONF%
goto :eof

:start
call %wrapper_bat% -t %_WRAPPER_CONF%
goto :eof

:stop
call %wrapper_bat% -p %_WRAPPER_CONF%
goto :eof

:query
call %wrapper_bat% -q %_WRAPPER_CONF%
goto :eof

:install
call %wrapper_bat% -i %_WRAPPER_CONF%
goto :eof

:remove
call %wrapper_bat% -r %_WRAPPER_CONF%
goto :eof

:restart
call %wrapper_bat% -p %_WRAPPER_CONF%
call %wrapper_bat% -t %_WRAPPER_CONF%
goto :eof

:exec
%*
goto :eof

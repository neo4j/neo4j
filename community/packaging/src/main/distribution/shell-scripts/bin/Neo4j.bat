@echo off
rem Copyright (c) 2002-2012 "Neo Technology,"
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

call:main %1

goto:eof

:main
set command=""
set serviceName="neo4j"
set serviceDisplayName="Neo4j Graph DB Server"
set serviceStartType=auto
call:parseConfig "%~dp0..\conf\neo4j-wrapper.conf"
for /F %%v in ('echo %1^|findstr "^help$ ^start$ ^stop$ ^query$ ^restart$ ^install$ ^remove$ ^console$"') do set command=%%v

if %command% == "" (
    set command=console
)

goto:%command%
if errorlevel 1 goto:callerror
goto:eof

:callerror
echo An error occurred in the process.
pause
goto:eof

:query
call:getStatus
echo %status%
goto:eof

:getStatus
set status=""

rem get the state line from the sc output
for /F "tokens=1,2 delims=:" %%a in ('sc query %serviceName%^|findstr "STATE"') do set status=%%b

rem Remove ALL space, concatenating the numerical and the string - no prob
set status=%status: =%

rem done, if non existent it will be empty
rem note that the strings we compare to normally
rem have two spaces between numerical and
rem string description. The substitution
rem above swallowed these, though
if "%status%" == "4RUNNING" (
	set status="RUNNING"
) else if "%status%" == "1STOPPED" (
	set status="STOPPED"
) else (
	set status="NOT INSTALLED"
)
goto:eof

:install
set classpath="-DserverClasspath=lib/*.jar;system/lib/*.jar;plugins/*.jar;system/coordinator/lib/*.jar"
set mainclass="-DserverMainClass=org.neo4j.server.Bootstrapper"
set binPath="java "-DworkingDir=%~dps0.." -DconfigFile=conf\neo4j-wrapper.conf %classpath% %mainclass% -jar "%~dps0windows-service-wrapper-1.jar" %serviceName%"
sc create %serviceName% binPath= %binPath% DisplayName= %serviceDisplayName% start= %serviceStartType%
call:start
goto:eof

:remove
call:stop
sc delete %serviceName%
goto:eof

:start
call:getStatus
if %status% == "NOT INSTALLED" (
	call:console
) else if %status% == "RUNNING" (
	echo Service is already running, no action taken
) else (
	sc start %serviceName%
)
goto:eof

:stop
sc stop %serviceName%
goto:eof

:restart
sc stop %serviceName%
sc start %serviceName%
goto:eof

:console
set classpath="-DserverClasspath=lib/*.jar;system/lib/*.jar;plugins/*.jar;system/coordinator/lib/*.jar"
set mainclass="-DserverMainClass=org.neo4j.server.Bootstrapper"
java "-DworkingDir=%~dp0.." -DconfigFile=conf\neo4j-wrapper.conf %classpath% %mainclass% -jar "%~dp0windows-service-wrapper-1.jar"
goto:eof

:help
echo Proper arguments for this command are: help start stop query restart install remove console
goto:eof

rem This function parses the various settings off the
rem configuration file. #'s are comments and ignored.

:parseConfig
set confFileName=%1

for /F "tokens=1,2 delims== eol=#" %%a in (%confFileName%) do call:setOption %%a %%b
goto:eof

rem Factored out switch for setting the variables
:setOption
	if "%1"=="wrapper.name" (
	set serviceName=%2
	) else if "%1"=="serviceDisplayName" (
	set serviceDisplayName=%2
	) else if "%1"=="serviceStartType" (
	set serviceStartType=%2
	)
goto:eof

@echo off
rem Copyright (c) 2002-2013 "Neo Technology,"
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

setlocal ENABLEEXTENSIONS

call:main %1

goto:eof

rem
rem function main
rem
:main

set settingError=""
call:checkSettings
if not %settingError% == "" (
  echo %settingError% variable is not set.
  goto:eof
)

call:findJavaHome
if not "%javaHomeError%" == "" (
  echo %javaHomeError%
  goto:eof
)

rem Unescape javaPath
for /f "tokens=* delims=" %%P in (%javaPath%) do (
    set javaPath=%%P
)

set wrapperJarFilename=${windows-wrapper.filename}
set command=""
call:parseConfig "%~dp0..\%configFile%"

for /F %%v in ('echo %1^|findstr "^help$ ^start$ ^stop$ ^status$ ^restart$ ^install$ ^uninstall$ ^remove$ ^console$"') do set command=%%v

if %command% == "" (
    if not "%1" == "" (
           echo This command is not supported by the Neo4j utility. Please try "Neo4j.bat help" for more info.
           goto:eof
    )
    set command=console
)

goto:%command%
if errorlevel 1 goto:callerror
goto:eof

rem end function main

rem
rem function findJavaHome
rem

:findJavaHome
if not "%JAVA_HOME%" == "" (
  
  if exist "%JAVA_HOME%\bin\javac.exe" (
    set javaPath= "%JAVA_HOME%\jre"
    goto:eof
  )

  set javaPath= "%JAVA_HOME%"
  goto:eof
)

rem Attempt finding JVM via registry
set keyName=HKLM\SOFTWARE\JavaSoft\Java Runtime Environment
set valueName=CurrentVersion

FOR /F "usebackq skip=2 tokens=3" %%a IN (`REG QUERY "%keyName%" /v %valueName% 2^>nul`) DO (
  set javaVersion=%%a
)

if "%javaVersion%" == "" (
  FOR /F "usebackq skip=2 tokens=3" %%a IN (`REG QUERY "%keyName%" /v %valueName% /reg:32 2^>nul`) DO (
    set javaVersion=%%a
  )
)

if "%javaVersion%" == "" (
  set javaHomeError=Unable to locate jvm. Could not find %keyName%/%valueName% entry in windows registry. Please make sure you either have %JAVA_HOME% environment variable defined and pointing to a JRE installation, or the registry key defined.
  goto:eof
)

set javaCurrentKey=HKLM\SOFTWARE\JavaSoft\Java Runtime Environment\%javaVersion%
set javaHomeKey=JavaHome

FOR /F "usebackq skip=2 tokens=2,*" %%a IN (`REG QUERY "%javaCurrentKey%" /v %javaHomeKey% 2^>nul`) DO (
  set javaPath="%%b"
)

if ""%javaPath% == "" (
  FOR /F "usebackq skip=2 tokens=2,*" %%a IN (`REG QUERY "%javaCurrentKey%" /v %javaHomeKey% /reg:32 2^>nul`) DO (
    set javaPath="%%b"
  )
)

goto:eof

rem end function findJavaHome

rem
rem function checkSettings
rem
:checkSettings
if "%serviceName%" == "" (
  set settingError=serviceName
  goto:eof
)
if "%serviceDisplayName%" == "" (
  set settingError=serviceDisplayName
  goto:eof
)
if %serviceStartType% == "" (
  serviceStartType=auto
)
if %classpath% == "" (
  set settingError=classpath
  goto:eof
)
if %mainclass% == "" (
  set settingError=mainclass
  goto:eof
)
if %configFile% == "" (
  set settingError=configFile
  goto:eof
)

goto:eof

rem end function checkSettings

rem
rem function callerror
rem
:callerror
echo An error occurred in the process.
pause
goto:eof

rem end function callerror

rem
rem function status
rem
:status
call:getStatus
echo %status%
goto:eof

rem end function status

rem
rem function getStatus
rem
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

rem end function getStatus


rem
rem function install
rem
:install
set binPath="%javaPath%\bin\java.exe -Djava.util.logging.config.file=conf\windows-wrapper-logging.properties -DworkingDir="%~dps0.." -DconfigFile=%configFile% %classpath% %mainclass% -Dorg.neo4j.cluster.logdirectory="%~dps0..\data\log" -jar %~dps0%wrapperJarFilename% %serviceName%"
sc create "%serviceName%" binPath= %binPath% DisplayName= "%serviceDisplayName%" start= %serviceStartType%
call:start
goto:eof

rem end function install


rem
rem function remove
rem
:uninstall
:remove
call:stop
sc delete %serviceName%
goto:eof

rem end function remove

rem
rem function start
rem
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

rem end function start


rem
rem function stop
rem
:stop
sc stop %serviceName%
goto:eof

rem end function stop


rem
rem function restart
rem
:restart
sc stop %serviceName%
sc start %serviceName%
goto:eof

rem end function restart


rem
rem function console
rem
:console
"%javapath%\bin\java.exe" -DworkingDir="%~dp0.." -Djava.util.logging.config.file=conf/windows-wrapper-logging.properties -DconfigFile=%configFile% %classpath% %mainclass% -jar %~dps0%wrapperJarFilename%
goto:eof

rem end function console


rem
rem function help
rem
:help
echo Proper arguments for this command are: help start stop status restart install remove console
goto:eof

rem end function help

rem
rem function parseConfig
rem
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

rem end function parseConfig


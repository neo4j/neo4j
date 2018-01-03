@echo off
rem Copyright (c) 2002-2018 "Neo Technology,"
rem Network Engine for Objects in Lund AB [http://neotechnology.com]
rem
rem This file is part of Neo4j.
rem
rem Neo4j is free software: you can redistribute it and/or modify
rem it under the terms of the GNU Affero General Public License as
rem published by the Free Software Foundation, either version 3 of the
rem License, or (at your option) any later version.
rem
rem This program is distributed in the hope that it will be useful,
rem but WITHOUT ANY WARRANTY; without even the implied warranty of
rem MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem GNU Affero General Public License for more details.
rem
rem You should have received a copy of the GNU Affero General Public License
rem along with this program. If not, see <http://www.gnu.org/licenses/>.

ECHO WARNING! This batch script has been deprecated. Please use the provided PowerShell scripts instead: http://neo4j.com/docs/stable/powershell.html 1>&2

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of agruments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dps0\..

:repoSetup

if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\lib

rem Setup the classpath
set LIBPATH=""
:GATHER_LIBPATH_1
IF NOT EXIST %REPO% GOTO GATHER_LIBPATH_2
pushd "%REPO%"
for %%G in (*.jar) do call:APPEND_TO_LIBPATH %%G
popd
:GATHER_LIBPATH_2
set REPO=%BASEDIR%\system\lib
IF NOT EXIST %REPO% GOTO GATHER_LIBPATH_3
pushd "%REPO%"
for %%G in (*.jar) do call:APPEND_TO_LIBPATH %%G
popd
:GATHER_LIBPATH_3
set REPO=%BASEDIR%\bin
IF NOT EXIST %REPO% GOTO LIBPATH_END
pushd "%REPO%"
for %%G in (*.jar) do call:APPEND_TO_LIBPATH %%G
popd
goto LIBPATH_END

: APPEND_TO_LIBPATH
set filename=%~1
set suffix=%filename:~-4%
if %suffix% equ .jar set LIBPATH=%LIBPATH%;"%REPO%\%filename%"
goto :EOF

:LIBPATH_END

set CLASSPATH=%LIBPATH%

set EXTRA_JVM_ARGUMENTS=-Dfile.encoding=UTF-8
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="neo4j-import" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" org.neo4j.tooling.ImportTool %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%

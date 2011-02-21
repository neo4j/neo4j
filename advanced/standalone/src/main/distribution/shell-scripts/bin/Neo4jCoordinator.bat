@echo off
setlocal

rem Copyright (c) 1999, 2010 Tanuki Software, Ltd.
rem http://www.tanukisoftware.com
rem All rights reserved.
rem
rem This software is the proprietary information of Tanuki Software.
rem You shall use it only in accordance with the terms of the
rem license agreement you entered into with Tanuki Software.
rem http://wrapper.tanukisoftware.com/doc/english/licenseOverview.html
rem
rem Java Service Wrapper command based script.

rem -----------------------------------------------------------------------------
rem These settings can be modified to fit the needs of your application
rem Optimized for use with version 3.5.4 of the Wrapper.

rem The base name for the Wrapper binary.
set _WRAPPER_BASE=wrapper

rem The name and location of the Wrapper configuration file.
rem  (Do not remove quotes.)
set _WRAPPER_CONF_DEFAULT="../conf/coord-wrapper.conf"

rem Do not modify anything beyond this point
rem -----------------------------------------------------------------------------

if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem Find the application home.
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

rem Decide on the wrapper binary.
rem Note: user is responsible for only keeping one
rem wrapper in place.
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-ia-64.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
echo %_REALPATH%%_WRAPPER_BASE%.exe
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF=""
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_WRAPPER_CONF_DEFAULT%"
goto validate

:validate
rem
rem Find the requested command.
rem
for /F %%v in ('echo %1^|findstr "^help$ ^console$ ^start$ ^pause$ ^resume$ ^stop$ ^restart$ ^install$ ^remove$"') do call :exec set COMMAND=%%v

if "%COMMAND%" == "" (
    set COMMAND=console
)

rem
rem Run the application.
rem At runtime, the current directory will be that of wrapper.exe
rem
call :%COMMAND%
if errorlevel 1 goto callerror
goto :eof

:callerror
echo An error occurred in the process.
pause
goto :eof

:help
echo Usage: %0 { console : start : stop : restart : install : remove } [command]
pause
goto :eof

:console
"%_WRAPPER_EXE%" -c %_WRAPPER_CONF%
goto :eof

:start
"%_WRAPPER_EXE%" -t %_WRAPPER_CONF%
goto :eof

:pause
"%_WRAPPER_EXE%" -a %_WRAPPER_CONF%
goto :eof

:resume
"%_WRAPPER_EXE%" -e %_WRAPPER_CONF%
goto :eof

:stop
"%_WRAPPER_EXE%" -p %_WRAPPER_CONF%
goto :eof

:install
"%_WRAPPER_EXE%" -i %_WRAPPER_CONF%
goto :eof

:remove
"%_WRAPPER_EXE%" -r %_WRAPPER_CONF%
goto :eof

:restart
call :stop
call :start
goto :eof

:exec
%*
goto :eof

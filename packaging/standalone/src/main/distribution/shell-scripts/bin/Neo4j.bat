@echo off
setlocal
call setenv.bat

rem This script is the main controller for the server
rem There are commands to install, uninstall, start, stop 
rem and restart the service on windows and also run it as
rem a console process. For the first four operations however,
rem Administrator rights are required. To enable the Administrator
rem account, issue in the command line
rem
rem net user Administrator /active:yes
rem net user Administrator <new passwd>
rem
rem and then to run a command from the console as admin,
rem
rem runas /env /user:Administrator Neo4j.bat install
rem
rem You will be prompted for the admin passwd and then
rem the service will be installed.

set _WRAPPER_CONF_DEFAULT="../conf/neo4j-wrapper.conf"
set _WRAPPER_CONF=%_WRAPPER_CONF_DEFAULT%

set _REALPATH=%~dp0


set _WRAPPER_EXE=%_REALPATH%wrapper.bat

for /F %%v in ('echo %1^|findstr "^help$ ^console$ ^start$ ^pause$ ^resume$ ^stop$ ^restart$ ^install$ ^remove$"') do call :exec set COMMAND=%%v

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
call %_REALPATH%runConsole.bat %_WRAPPER_CONF%
goto :eof

:start
call %_REALPATH%startService.bat %_WRAPPER_CONF%
goto :eof

:stop
call %_REALPATH%stopService.bat %_WRAPPER_CONF%
goto :eof

:install
call %_REALPATH%installService.bat %_WRAPPER_CONF%
goto :eof

:remove
call %_REALPATH%uninstallService.bat %_WRAPPER_CONF%
goto :eof

:restart
call %_REALPATH%stopService.bat %_WRAPPER_CONF%
call %_REALPATH%startService.bat %_WRAPPER_CONF%
goto :eof

:exec
%*
goto :eof

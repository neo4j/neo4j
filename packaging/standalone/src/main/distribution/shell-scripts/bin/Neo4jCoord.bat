@echo off

rem
rem This script is the main controller for the server coordinator
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
rem bin\Neo4jCoord.bat install
rem
rem to install Neo4j as a Windows Service.
rem

setlocal
call "%~dp0"setenv.bat

set _WRAPPER_CONF_DEFAULT="..\conf\coord-wrapper.conf"
set _WRAPPER_CONF=%_WRAPPER_CONF_DEFAULT%

set _REALPATH=%~dp0


set _WRAPPER_EXE="%_REALPATH%wrapper.bat"

for /F %%v in ('echo %1^|findstr "^help$ ^console$ ^start$ ^stop$ ^restart$ ^query$ ^install$ ^remove$"') do call :exec set COMMAND=%%v

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

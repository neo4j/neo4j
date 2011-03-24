@echo off
setlocal
call setenv.bat

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

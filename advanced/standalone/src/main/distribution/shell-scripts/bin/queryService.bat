cd %~dp0
call setenv.bat
%wrapper_bat% -q %conf_file%
pause


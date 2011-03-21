cd %~dp0
call setenv.bat
%wrapper_bat% -r %conf_file%
pause
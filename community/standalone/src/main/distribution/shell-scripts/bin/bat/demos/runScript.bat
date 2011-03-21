rem usage: runScript.bat <config file> <script file> [<count>]

call setenv.bat
%java_exe% -cp %wrapper_jar% org.rzo.yajsw.script.RunScriptBooter %1 %2 %3
pause
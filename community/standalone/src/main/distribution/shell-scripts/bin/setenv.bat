@echo off
rem quotes are required for correct handling of path with spaces

rem default java home
set wrapper_home=%~dp0/..

rem default java exe for running the wrapper
rem note this is not the java exe for running the application. the exe for running the application is defined in the wrapper configuration file
set java_exe="java"

rem location of the wrapper jar file. necessary lib files will be loaded by this jar. they must be at <wrapper_home>/lib/...
set wrapper_jar="%wrapper_home%/wrapper.jar"

rem setting java options for wrapper process. depending on the scripts used, the wrapper may require more memory.
set wrapper_java_options=-Xmx30m

rem wrapper bat file for running the wrapper
set wrapper_bat="%wrapper_home%/bat/wrapper.bat"

rem configuration file used by all bat files
set conf_file="%wrapper_home%/conf/wrapper.conf"

rem default configuration used in genConfig
set conf_default_file="%wrapper_home%/conf/wrapper.conf.default"

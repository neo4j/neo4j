@echo off
rem Copyright (c) 2002-2011 "Neo Technology,"
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

rem quotes are required for correct handling of path with spaces

rem default java home
set wrapper_home=%~dp0

rem default java exe for running the wrapper
rem note this is not the java exe for running the application. the exe for running the application is defined in the wrapper configuration file
set java_exe="java"

rem location of the wrapper jar file. necessary lib files will be loaded by this jar. they must be at <wrapper_home>/lib/...
set wrapper_jar="%wrapper_home%\wrapper.jar"

rem setting java options for wrapper process. depending on the scripts used, the wrapper may require more memory.
set wrapper_java_options=-Xmx30m

rem the location of the java.util.logging properties file to funnel all wrapper carried libs logging to a safe place
set wrapper_log_options="-Djava.util.logging.config.file=%wrapper_home%..\conf\wrapper-logging.properties"

rem wrapper bat file for running the wrapper
set wrapper_bat="%wrapper_home%\wrapper.bat"

rem configuration file used by all bat files
set conf_file=%_WRAPPER_CONF%

rem default configuration used in genConfig
set conf_default_file=%_WRAPPER_CONF_DEFAULT%

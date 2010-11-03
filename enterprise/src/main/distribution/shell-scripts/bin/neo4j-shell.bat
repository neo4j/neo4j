@REM ----------------------------------------------------------------------------
@REM Copyright 2001-2004 The Apache Software Foundation.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM ----------------------------------------------------------------------------
@REM

@echo off

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
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\/lib

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\neo4j-kernel-1.2-20101102.005253-124.jar;"%REPO%"\geronimo-jta_1.1_spec-1.1.1.jar;"%REPO%"\neo4j-index-1.2-20101102.071526-163.jar;"%REPO%"\org.apache.servicemix.bundles.lucene-3.0.1_2.jar;"%REPO%"\neo4j-lucene-index-0.2-20101102.124246-66.jar;"%REPO%"\neo4j-shell-1.2-20101102.123804-162.jar;"%REPO%"\org.apache.servicemix.bundles.jline-0.9.94_1.jar;"%REPO%"\neo4j-remote-graphdb-0.8-20101102.124424-143.jar;"%REPO%"\protobuf-java-2.3.0.jar;"%REPO%"\neo4j-graph-algo-0.7-20101102.005429-135.jar;"%REPO%"\neo4j-online-backup-0.7-20101102.124324-142.jar;"%REPO%"\neo4j-udc-0.1-20101102.005450-51.jar;"%REPO%"\neo4j-management-1.2-SNAPSHOT.jar;"%REPO%"\neo4j-rest-0.8-20101102.143839-73.jar;"%REPO%"\jersey-server-1.3.jar;"%REPO%"\jersey-core-1.3.jar;"%REPO%"\jsr311-api-1.1.1.jar;"%REPO%"\asm-3.1.jar;"%REPO%"\oauth-server-1.3.jar;"%REPO%"\oauth-signature-1.3.jar;"%REPO%"\jackson-jaxrs-1.4.1.jar;"%REPO%"\jackson-core-asl-1.4.1.jar;"%REPO%"\jackson-mapper-asl-1.4.1.jar;"%REPO%"\grizzly-servlet-webserver-1.9.18.jar;"%REPO%"\grizzly-http-1.9.18.jar;"%REPO%"\grizzly-framework-1.9.18.jar;"%REPO%"\grizzly-utils-1.9.18.jar;"%REPO%"\grizzly-rcm-1.9.18.jar;"%REPO%"\grizzly-portunif-1.9.18.jar;"%REPO%"\grizzly-http-servlet-1.9.18.jar;"%REPO%"\servlet-api-2.5.jar;"%REPO%"\jetty-6.1.25.jar;"%REPO%"\jetty-util-6.1.25.jar;"%REPO%"\servlet-api-2.5-20081211.jar;"%REPO%"\neo4j-server-0.1-SNAPSHOT.jar;"%REPO%"\log4j-1.2.16.jar;"%REPO%"\commons-configuration-1.6.jar;"%REPO%"\commons-collections-3.2.1.jar;"%REPO%"\commons-lang-2.4.jar;"%REPO%"\commons-logging-1.1.1.jar;"%REPO%"\commons-digester-1.8.jar;"%REPO%"\commons-beanutils-1.7.0.jar;"%REPO%"\commons-beanutils-core-1.8.0.jar;"%REPO%"\commons-io-1.4.jar;"%REPO%"\neo4j-examples-1.2-SNAPSHOT.jar;"%REPO%"\neo4j-udc-0.1-20101102.005450-51-neo4j.jar;"%REPO%"\neo4j-examples-1.2-SNAPSHOT-sources.jar;"%REPO%"\neo4j-examples-1.2-SNAPSHOT-test-sources.jar;"%REPO%"\neo4j-standalone-0.1-SNAPSHOT.pom
set EXTRA_JVM_ARGUMENTS=
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="neo4j-shell" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" org.neo4j.shell.StartClient %CMD_LINE_ARGS%
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

@echo off
rem
rem function install
rem

setlocal ENABLEEXTENSIONS

set serviceName=Neo4j-Server
set serviceDisplayName="Neo4j Graph Database"
set serviceStartType=auto
set classpath="-DserverClasspath=lib/*.jar;system/lib/*.jar;plugins/**/*.jar;./conf*"
set mainclass="-DserverMainClass=org.neo4j.server.Bootstrapper"
set configFile="conf\neo4j-wrapper.conf"
set loggingProperties="-Djava.util.logging.config.file=conf\windows-wrapper-logging.properties"
set wrapperJarFilename=windows-service-wrapper-5.jar

goto :main %1



:getStatus
  set status=""

  rem get the state line from the sc output
  for /F "tokens=1,2 delims=:" %%a in ('sc query %serviceName%^|findstr "STATE"') do set status=%%b

  rem Remove ALL space, concatenating the numerical and the string - no prob
  set status=%status: =%

  rem done, if non existent it will be empty
  rem note that the strings we compare to normally
  rem have two spaces between numerical and
  rem string description. The substitution
  rem above swallowed these, though
  if "%status%" == "4RUNNING" (
    set status="RUNNING"
  ) else if "%status%" == "1STOPPED" (
    set status="STOPPED"
  ) else (
    set status="NOT INSTALLED"
  )
  goto:eof

:status
  call :getStatus
  echo %status%
  goto :eof

:install
  call "%~dps0functions.bat" :findJavaHome
  if not "%javaHomeError%" == "" (
    echo %javaHomeError%
    call:instructions
    goto:eof
  )

  call:verifySupportedJavaVersion
  if not "%javaVersionError%" == "" (
    echo %javaVersionError%
    call:instructions
    goto:eof
  )
  rem Remove quotes from javaPath so that it can have /bin/java appended to it
  rem See http://ss64.com/nt/syntax-esc.html, "Removing quotes"
  set javaPath=###%javaPath%###
  set javaPath=%javaPath:"###=%
  set javaPath=%javaPath:###"=%
  set javaPath=%javaPath:###=%

  set binPath="\"%javaPath%\bin\java.exe\" %loggingProperties% -DworkingDir="%~dps0.." -DconfigFile=%configFile% %classpath% %mainclass% -Dorg.neo4j.cluster.logdirectory="%~dps0..\data\log" -jar %~dps0%wrapperJarFilename%  %serviceName%"

  sc create "%serviceName%" binPath= %binPath% DisplayName= "%serviceDisplayName:"=%" start= %serviceStartType%
  sc start %serviceName%
  @echo off
  goto :eof


:remove
  call :stop
  sc delete %serviceName%
  goto :eof

:start
  call :getStatus
  if %status% == "NOT INSTALLED" (
    call:console
  ) else if %status% == "RUNNING" (
    echo Service is already running, no action taken
  ) else (
    sc start %serviceName%
  )
  goto :eof

:stop
  sc stop %serviceName%
  goto:eof

:restart
  call :stop
  call :start
  goto :eof

:usage
  echo Usage: %~0Neo4jInstaller.bat ^<install^|remove^>
  goto:eof

:main
  if "%1" == "" goto :usage
  if "%1" == "remove"  goto :remove
  if "%1" == "install" goto :install
  if "%1" == "stop" goto :stop
  if "%1" == "start" goto :start
  if "%1" == "status" goto :status
  call :usage
  goto :eof


rem end function remove

:verifySupportedJavaVersion

  set javaVersionError=
  set javaCommand=%javaPath:"=%\bin\java.exe

  rem Remove leading spaces
  for /f "tokens=* delims= " %%a in ("%javaCommand%") do set javaCommand=%%a

  rem Find version
  for /f "usebackq tokens=3" %%g in (`"%javaCommand%" -version 2^>^&1`) do (
      set JAVAVER=%%g
      goto:breakJavaVersionLoop
  )
  :breakJavaVersionLoop

  set JAVAVER=%JAVAVER:"=%
  set "JAVAVER=%JAVAVER:~0,3%"

  if "%JAVAVER%"=="1.7" goto:eof
  if "%JAVAVER%"=="1.8" (
    echo ERROR! You are using an unsupported version of Java, please use Oracle HotSpot 1.7.
    goto:eof
  )
  set javaVersionError=ERROR! You are using an unsupported version of Java, please use Oracle HotSpot 1.7.
  goto:eof

:instructions
  echo * Please use Oracle(R) Java(TM) 7 to run Neo4j Server. Download "Java Platform (JDK) 7" from:
  echo   http://www.oracle.com/technetwork/java/javase/downloads/index.html
  echo * Please see http://docs.neo4j.org/ for Neo4j Server installation instructions.
  goto:eof

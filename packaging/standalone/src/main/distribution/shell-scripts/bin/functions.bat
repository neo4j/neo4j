rem
rem function findJavaHome
rem

:findJavaHome
  if not "%JAVA_HOME%" == "" (
    
    if exist "%JAVA_HOME%\bin\javac.exe" (
      set javaPath="%JAVA_HOME%\jre"
      goto:eof
    )

    set javaPath="%JAVA_HOME%"
    goto:eof
  )

  rem Attempt finding JVM via registry
  set keyName=HKLM\SOFTWARE\JavaSoft\Java Runtime Environment
  set valueName=CurrentVersion

  FOR /F "usebackq skip=2 tokens=3" %%a IN (`REG QUERY "%keyName%" /v %valueName% 2^>nul`) DO (
    set javaVersion=%%a
  )

  if "%javaVersion%" == "" (
    FOR /F "usebackq skip=2 tokens=3" %%a IN (`REG QUERY "%keyName%" /v %valueName% /reg:32 2^>nul`) DO (
      set javaVersion=%%a
    )
  )

  if "%javaVersion%" == "" (
    set javaHomeError=ERROR! Unable to locate Java. Could not find %keyName%/%valueName% entry in windows registry. Please make sure you either have JAVA_HOME environment variable defined and pointing to a JRE installation, or the registry key defined.
    goto:eof
  )

  set javaCurrentKey=HKLM\SOFTWARE\JavaSoft\Java Runtime Environment\%javaVersion%
  set javaHomeKey=JavaHome

  FOR /F "usebackq skip=2 tokens=2,*" %%a IN (`REG QUERY "%javaCurrentKey%" /v %javaHomeKey% 2^>nul`) DO (
    set javaPath="%%b"
  )

  if ""%javaPath% == "" (
    FOR /F "usebackq skip=2 tokens=2,*" %%a IN (`REG QUERY "%javaCurrentKey%" /v %javaHomeKey% /reg:32 2^>nul`) DO (
      set javaPath="%%b"
    )
  )

  goto:eof

rem end function findJavaHome


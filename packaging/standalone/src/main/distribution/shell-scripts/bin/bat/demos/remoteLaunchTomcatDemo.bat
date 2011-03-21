rem launches tomcat which is installed on http://eprognos.com/webdav using YAJSW
rem you can check that tomcat is running on http://localhost:8081

call setenv.bat
"%wrapper_home%\bat\wrapper.bat" -c http://eprognos.com/webdav/wrapper.tomcat.conf
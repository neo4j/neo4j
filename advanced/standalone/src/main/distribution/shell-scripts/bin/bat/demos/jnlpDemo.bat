rem runs sun jnlp draw demo using YAJSW instead of java web start

call setenv.bat
"%wrapper_home%\bat\wrapper.bat" -c -d %wrapper_home%/conf/wrapper.javaws.conf http://java.sun.com/javase/technologies/desktop/javawebstart/apps/draw.jnlp
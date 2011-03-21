#!/bin/bash
# -----------------------------------------------------------------------------
# Launch tomcat which is installed on http://eprognos.com/webdav using YAJSW
# To check that tomcat is running: http://localhost:8081
# -----------------------------------------------------------------------------

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done
 
PRGDIR=`dirname "$PRG"`
EXECUTABLE=wrapper.sh

# set java and conf file
source "$PRGDIR"/setenv.sh
export PRGDIR

# Check that target executable exists
if [ ! -x "$PRGDIR"/"$EXECUTABLE" ]; then
  echo "Cannot find $PRGDIR/$EXECUTABLE"
  echo "This file is needed to run this program"
  exit 1
fi

exec "$PRGDIR"/"$EXECUTABLE" -c http://eprognos.com/webdav/wrapper.tomcat.conf
 

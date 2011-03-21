#!/bin/bash
# -----------------------------------------------------------------------------
# run test.HelloWorld
#
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

# set java and conf file
source "$PRGDIR"/setenv.sh

"$java_exe" "$yajsw_java_options" -cp "$yajsw_jar" test.HelloWorld 


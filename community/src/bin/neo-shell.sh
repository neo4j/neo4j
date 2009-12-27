#!/bin/sh
#
# Script to start the Neo4j shell.
#
#

SCRIPTDIR=`dirname "$0"`

java $JAVA_OPTS -cp $SCRIPTDIR:$SCRIPTDIR/neo-${neo.version}.jar:$SCRIPTDIR/geronimo-jta_${jta.version}_spec-1.1.1.jar:$SCRIPTDIR/shell-${project.version}.jar:$SCRIPTDIR/jline-${jline.version}.jar org.neo4j.shell.StartLocalClient "$@"

#!/bin/bash
# -----------------------------------------------------------------------------
# Set java exe and conf file for all scripts
#
# -----------------------------------------------------------------------------

# resolve links - $0 may be a softlink

current=`pwd`
cd `dirname "$0"`
PRGDIR=`pwd`
cd "$current"

yajsw_home="$PRGDIR"/..
export yajsw_home

yajsw_jar="$yajsw_home"/bin/wrapper.jar
export yajsw_jar

yajsw_java_options=-Xmx15m
export yajsw_java_options

java_exe=java
export java_exe

# show java version
"$java_exe" -version

conf_file="$yajsw_home"/conf/neo4j-wrapper.conf
export conf_file

conf_default_file="$yajsw_home"/conf/wrapper.conf.default
export conf_default_file




#!/bin/sh
# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

""":"
# Locate (good enough) absolute path for the Neo4j library
FILE=$0
while [ -L "$FILE" ]; do
    FILE=$(readlink $FILE)
done
DIR=$(cd `dirname $FILE`; pwd)
FILE=$(basename $FILE)
if [ "__main__.py" != "$FILE" ]; then
    echo Neo4j library has been obscured >&2
    exit -1 
fi
while [ -L "$DIR" ]; do
    DIR=$(readlink $DIR)
done
if [ "neo4j" != "$(basename $DIR)" ]; then
    echo Neo4j library has been obscured >&2
    exit -1
fi
DIR=$(dirname $DIR)
while [ -L "$DIR" ]; do
    DIR=$(readlink $DIR)
done

# Set up the Python path
if [ -z "$PYTHONPATH" ]; then
    PYTHONPATH="$DIR"
else
    PYTHONPATH="$DIR:$PYTHONPATH"
fi
export PYTHONPATH

BIN=$(dirname $(dirname $DIR))/bin
if [ -d "$BIN" -a -f "$BIN/classpath" -a -x "$BIN/classpath" ]; then
    CLASSPATH=$($BIN/classpath)
    export CLASSPATH
fi

# Launch the __main__
python "$DIR/neo4j/$FILE" "$@"
# Exit with same exit code
exit $?
":"""
USAGE = """USAGE: neo4j <graphdb path> [<script> ...]

Where   <graphdb  path>   is  the   path  to   your   graph  database,
and <script> is a script to execute. You can provide multiple scripts,
if no script is provided, an interactive (Python) shell is started.
"""

if __name__ != '__main__':
    raise ImportError(
        "neo4j.__main__ is not meant to be imported."
        "It is meant to be used as an executable.\n\n" + USAGE)
else:
   import sys, os

   path = sys.argv[0]
   if not os.path.isdir(path):
       path = os.path.dirname(path)
   if path in sys.path:
       sys.path.remove(path)
   path = os.path.dirname(os.path.abspath(path))
   if path not in sys.path:
       sys.path.insert(0, path)

   import neo4j

   def run(script, env):
       exec(script, env)

   def script(infile):
       def main(env):
           run(infile.read(), env)
       return main

   def scripts(paths):
       for path in paths:
           if not os.path.isfile(path):
               raise TypeError("Script <%s> does not exits." % (path,))
       def main(env):
           for path in paths:
               with open(path) as infile:
                   script(infile)(dict(env))
       return main

   def interactive(env):
       from code import InteractiveConsole
       InteractiveConsole(locals=env).interact(
           banner="Neo4j Python bindings interactive console")

   try:
       if len(sys.argv) > 2:
           main = scripts(sys.argv[2:])
       elif len(sys.argv) < 2:
           raise TypeError("No database path specified")
       elif not sys.stdin.isatty():
           main = script(sys.stdin)
       else:
           main = interactive

       if os.path.isfile(sys.argv[1]):
           raise TypeError("Database path is a file")
       elif not os.path.exists(sys.argv[1]):
           sys.stderr.write("WARNING: Database path <%s> does not exist, a "
                            "Neo4j database will be created.\n"%(sys.argv[1],))
   except:
       sys.stderr.write(str(sys.exc_info()[1]) + '\n\n')
       sys.stderr.write(USAGE)
       sys.exit(-1)

   graphdb = neo4j.GraphDatabase(sys.argv[1])
   try:
       main({
           'graphdb': graphdb,
       })
   finally:
       graphdb.shutdown()

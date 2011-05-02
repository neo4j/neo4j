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
# If Neo4j Python bindings are installed: use the installed ones
if ! python -c "import neo4j" &> /dev/null; then
    # Otherwise: set up PYTHONPATH to use the checked out source
    SRC=$0
    for (( c=4; c>0; c-- )); do
        while [ -L "$SRC" ]; do
            SRC=$(readlink $SRC)
        done
        SRC=$(cd $(dirname $SRC); pwd)
    done

    if [ -z "$PYTHONPATH" ]; then
        PYTHONPATH="$SRC/main/python"
    else
        PYTHONPATH="$PYTHONPATH:$SRC/main/python"
    fi
    export PYTHONPATH
    
    CLASSPATH=$($SRC/bin/classpath)
    if [ $? -ne 0 ]; then exit -1; fi
    export CLASSPATH
fi

python $0 "$@"
exit $?
":"""

try:
    from django.core.management import execute_manager
except ImportError:
    import sys
    sys.stderr.write("Error: Django is not available. Django must be installed to run this sample.")
    sys.exit(1)

try:
    import settings # Assumed to be in the same directory.
except ImportError:
    import sys
    sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. It appears you've customized things.\nYou'll have to run django-admin.py, passing it your settings module.\n(If the file settings.py does indeed exist, it's causing an ImportError somehow.)\n" % __file__)
    sys.exit(1)

if __name__ == "__main__":
    execute_manager(settings)

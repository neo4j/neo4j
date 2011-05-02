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

from distutils.core import setup, Command
from os.path import abspath, dirname, join as pathjoin

class depend(Command):
    pass

def dependencies(pom,seen=None):
    if seen is None: seen = {} # don't download the same dependency twice
    # 1. Get parent pom (recursively)
    # 2. Get dependency management from parent pom
    # 3. Get all dependencies for this project (pom and jar)
    # 4. For each dependency, call this function recursively
    # --- for now - fake it by forking to mvn :) ---
    import subprocess
    subprocess.call(["mvn", "dependency:copy-dependencies"], cwd=dirname(pom))
    print pathjoin(dirname(pom),"target","dependency")

if __name__ == '__main__':
    #setup(cmdclass={'depend':depend},)
    pom = pathjoin(dirname(dirname(dirname(dirname(abspath(__file__))))),
                   'pom.xml')
    dependencies(pom)

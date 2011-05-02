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

import django
if django.VERSION < (1,2):
    raise ImportError("Neo4j Django support requires Django version >= 1.2")

import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'neo4j_django_tests._settings'

from django.test.utils import setup_test_environment
setup_test_environment()

import unittest

_modules = {}

for candidate in os.listdir(os.path.dirname(os.path.abspath(__file__))):
    if candidate.endswith('.py') and not candidate.startswith('_'):
        candidate = candidate[:-3]
        try:
            exec("from %s import *" % candidate)
        except:
            _modules[candidate] = traceback.format_exc()
        else:
            _modules[candidate] = None

class ImportDjangoTestModules(unittest.TestCase):
    for _module in _modules:
        if _modules[_module] is None:
            def _test(self):
                pass
        else:
            def _test(self,name=_module,failure=_modules[_module]):
                self.fail('Failed to import test module "%s"\n%s'
                          % (name, failure))
        exec("test_import_%s = _test" % _module)

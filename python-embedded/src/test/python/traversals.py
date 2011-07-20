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

from __future__ import with_statement

__all__ = 'SomeTests',

import unit_tests, datetime

from neo4j import *

class SomeTests(unit_tests.GraphDatabaseTest):
    def test_basic_traversal(self):
        with self.graphdb.transaction:
            source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = source.related_to(target, message="graphy")
        
        paths = list(source / Any('related_to'))
        
        self.assertEqual(1, len(paths))
        
        targetpath = paths[0]
        
        target = targetpath.endNode()
        self.assertEquals(target['message'], 'world')
        

if __name__ == '__main__':
    unit_tests.unittest.main()

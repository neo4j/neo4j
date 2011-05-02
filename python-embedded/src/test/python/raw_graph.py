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

import unit_tests

class GraphTest(unit_tests.GraphDatabaseTest):
    def test_can_create_node(self):
        with self.graphdb.transaction:
            node = self.graphdb.node(name='Thomas Anderson', age=42)
        self.assertNotEqual(node, None)
        self.assertEquals(node['name'], 'Thomas Anderson')
        self.assertEquals(node['age'], 42)
        
    def test_can_create_relationship(self):
        with self.graphdb.transaction:
            source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = source.related_to(target, message="graphy")
        message = ''
        for rel in source.related_to:
            message += "%s %s %s" % (
                rel.startNode['message'],
                rel['message'],
                rel.endNode['message'],
                )
        self.assertEquals(message, "hello graphy world")

if __name__ == '__main__':
    unit_tests.unittest.main()

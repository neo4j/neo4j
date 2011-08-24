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
import neo4j
from neo4j import Direction, Evaluation, Uniqueness

class IndexTest(unit_tests.GraphDatabaseTest):

    def test_index_node(self):
        with self.graphdb.transaction:
            self.source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = self.source.related_to(target, message="graphy")
            secondrel = target.likes(self.source, message="buh")
        
        
if __name__ == '__main__':
    unit_tests.unittest.main()

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
import tempfile, os

class CypherTest(unit_tests.GraphDatabaseTest):
        
    def test_simple_query(self):
        db = self.graphdb
        
        # START SNIPPET: basicCypherQuery
        result = db.query("START n=node(0) RETURN n")
        # END SNIPPET: basicCypherQuery
        
        # Fetch an iterator for the "n" column
        col = result['n']
        
        # We know its a single result
        node = col.single
        
        self.assertEquals(0, node.id)
        
        # So we could have done this
        node = result['n'].single
        
        self.assertEquals(0, node.id)
        
        # Iterate through all result rows
        for row in result:
            node = row['n']
            self.assertEquals(0, node.id)
        
    def test_list_columns(self):
        db = self.graphdb
        
        # START SNIPPET: listCypherResultColumns
        result = db.query("START n=node(0) RETURN n,count(n)")
        
        # Get a list of the column names
        columns = result.keys()
        # END SNIPPET: listCypherResultColumns
        
        self.assertEquals(columns[0], 'n')
        self.assertEquals(columns[1], 'count(n)')
        
        
if __name__ == '__main__':
    unit_tests.unittest.main()

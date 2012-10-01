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
        
    #def test_simple_query(self):
    #    db = self.graphdb
    #    
    #    # START SNIPPET: basicCypherQuery
    #    result = db.query("START n=node(0) RETURN n")
    #    # END SNIPPET: basicCypherQuery
    #    
    #    # START SNIPPET: getCypherResultColumn
    #    root_node = "START n=node(0) RETURN n"
    #    
    #    # Fetch an iterator for the "n" column
    #    column = db.query(root_node)['n']
    #    
    #    for cell in column:
    #        node = cell
    #    
    #    # Coumns support "single":
    #    column = db.query(root_node)['n']
    #    node = column.single
    #    # END SNIPPET: getCypherResultColumn
    #    
    #    self.assertEquals(0, node.id)
    #    
    #    # START SNIPPET: iterateCypherResult
    #    root_node = "START n=node(0) RETURN n"
    #    
    #    # Iterate through all result rows
    #    for row in db.query(root_node):
    #        node = row['n']
    #        
    #    # We know it's a single result,
    #    # so we could have done this as well
    #    node = db.query(root_node).single['n']
    #    # END SNIPPET: iterateCypherResult
    #    self.assertEquals(0, node.id)
    #    
    #def test_list_columns(self):
    #    db = self.graphdb
    #    
    #    # START SNIPPET: listCypherResultColumns
    #    result = db.query("START n=node(0) RETURN n,count(n)")
    #    
    #    # Get a list of the column names
    #    columns = result.keys()
    #    # END SNIPPET: listCypherResultColumns
    #    
    #    self.assertEquals(columns[0], 'n')
    #    self.assertEquals(columns[1], 'count(n)')
    #    
    #def test_parameterized_query(self):
    #    db = self.graphdb
    #    
    #    # START SNIPPET: parameterizedCypherQuery
    #    result = db.query("START n=node({id}) RETURN n",id=0)
    #    
    #    node = result.single['n']
    #    # END SNIPPET: parameterizedCypherQuery
    #    
    #    self.assertEquals(0, node.id)
    #    
    #def test_prepared_queries(self):
    #    db = self.graphdb
    #    
    #    # START SNIPPET: preparedCypherQuery
    #    get_node_by_id = db.prepare_query("START n=node({id}) RETURN n")
    #    
    #    result = db.query(get_node_by_id, id=0)
    #    
    #    node = result.single['n']
    #    # END SNIPPET: preparedCypherQuery
    #    
    #    self.assertEquals(0, node.id)
        
    def test_aggregate_queries(self):
        db = self.graphdb

        with db.transaction:
              node = db.query('''CREATE node-[:Depends_on]->port<-[:Has]-parent1<-[:Has]-parent2 
                                 RETURN node''').single['node']

        result = db.query('''START node=node({id})
                MATCH p=node-[:Depends_on]->port<-[:Has*1..]-parent
                RETURN node, COLLECT(p) as end_points''', id=node.id)

        collected = result['end_points']
        for val in collected:
            pass
        
        
        
        
if __name__ == '__main__':
    unit_tests.unittest.main()

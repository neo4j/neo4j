# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2013 "Neo Technology,"
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

import threading
        

class ThreadingTest(unit_tests.GraphDatabaseTest):

    def test_write_in_one_thread_read_in_another(self):
        db = self.graphdb
        
        with db.transaction:
            node = db.node(name="Bob the node")
            
        def read_method():
            self.assertEquals(db.node[node.id]['name'], "Bob the node")
            
        thread = threading.Thread(target=read_method)
        thread.start()
        thread.join()

    def test_create_db_in_one_thread_read_index_in_another(self):
        db = self.graphdb
        
        with db.transaction:
            node = db.node(name="Bob the node")
            node_idx = db.node.indexes.create('my_nodes', type='fulltext')
            node_idx['akey']['avalue'] = node
            
        def read_method():
            node_idx = db.node.indexes.get('my_nodes') 
            node = node_idx['akey']['avalue'][0]
            self.assertEquals(node['name'], "Bob the node")
            
        thread = threading.Thread(target=read_method)
        thread.start()
        thread.join()
        
if __name__ == '__main__':
    unit_tests.unittest.main()

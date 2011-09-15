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

"""Python bindings for the embedded Neo4j Graph Database.
"""

__all__ = 'GraphDatabase',\
          'Direction', 'Evaluation', 'Uniqueness', 'BOTH', 'ANY', 'INCOMING', 'OUTGOING'
          
__version__ = "${pythonic_version}"

from neo4j.core import GraphDatabase, Direction, NotFoundException, BOTH, ANY, INCOMING, OUTGOING
from neo4j.traversal import Traversal, Evaluation, Uniqueness
from neo4j.index import NodeIndexManager, RelationshipIndexManager
from neo4j.util import rethrow_current_exception_as

class Nodes(object):
    
    def __init__(self, db):
        self.db = db
        self.indexes = NodeIndexManager(db)
    
    def __call__(self, **properties):
        return self.create(**properties)
        
    def __getitem__(self, items):
        return self.get(items)
            
    def __delitem__(self, item):
        return self[item].delete()
        
    def create(self, **properties):
        node = self.db.createNode()
        for key, val in properties.items():
            node[key] = val
        return node
        
    def get(self, id):
        if not isinstance(id, (int, long)):
            raise TypeError("Only integer and long values allowed as node ids.")
        try:
            return self.db.getNodeById( id )
        except:
            rethrow_current_exception_as(KeyError)
           

class Relationships(object):
    
    def __init__(self, db):
        self.db = db
        self.indexes = RelationshipIndexManager(db)
        
    def __getitem__(self, items):
        return self.get(items)
            
    def __delitem__(self, item):
        return self[item].delete()
        
    def get(self, id):
        if not isinstance(id, (int, long)):
            raise TypeError("Only integer and long values allowed as relationship ids.")
            
        try:
            return self.db.getRelationshipById( id )
        except:
            rethrow_current_exception_as(KeyError)


class GraphDatabase(GraphDatabase):
    
    from core import __new__
    
    try:
        from contextlib import contextmanager
    except:
        pass
    else:
        ## yield in try is a recent feature in Python,
        ## guard with exec to support old versions of Python
        ## This should pass since import contextlib has worked
        exec('''def transaction(self):
        """Allows usage of the with-statement for Neo4j transactions:
        with graphdb.transaction:
            doMutatingOperations()
        """
        tx = self.beginTx()
        try:
            yield tx
            tx.success()
        finally:
            tx.finish()
''')
        transaction = property(contextmanager(transaction))
        del contextmanager # from the body of this class
    
    @property
    def node(self):
        if not hasattr(self, '__nodes'):
            self.__nodes = Nodes(self)
        return self.__nodes
        
    # Syntax sugar
    nodes = node
        
    @property
    def relationship(self):
        if not hasattr(self, '__relationships'):
            self.__relationships = Relationships(self)
        return self.__relationships
        
    # Syntax sugare
    relationships = relationship
        
    @property
    def reference_node(self):
        return self.getReferenceNode()
    
    def traversal(self):
        return Traversal.description()
        


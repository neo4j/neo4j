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

"""Python bindings for the Neo4j Graph Database.
"""

__all__ = 'GraphDatabase', 'Node', 'Relationship',\
          'Property', 'incoming', 'outgoing', 'any',\
          'Incoming', 'Outgoing', 'Any'

from neo4j._backend import GraphDatabase

class GraphDatabase(GraphDatabase):
    from neo4j._backend import __new__

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

    def node(self, **properties):
        node = self.createNode()
        for key, val in properties.items():
            node[key] = val
        return node

# modules imported per default
from neo4j.model import *
from neo4j.traversal import *
try:
    from neo4j.algo import *
except:
    pass


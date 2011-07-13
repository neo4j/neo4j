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

from neo4j import Node, Relationship, Property

class Friendship(Relationship):

    introduced = Property(datetime.datetime, default=None)
    
class Person(Node):
    friends = Friendship('Person',type='KNOWS')
    name = Property(str, indexed=True)
    birthday = Property(datetime.datetime, default=None)

class SomeTests(unit_tests.GraphDatabaseTest):
    def test_find_Person(self):
        with self.graphdb.transaction:
            Person(self.graphdb, name='Thomas Anderson',
                   birthday=datetime.datetime(1978,05,21))
        neo = Person.find(self.graphdb, name='Thomas Anderson').single
        self.assertFalse(neo is None)
        self.assertEqual('Thomas Anderson', neo.name)
        self.assertEqual(datetime.datetime(1978,05,21), neo.birthday)

    def test_relate_Person(self):
        with self.graphdb.transaction:
            cypher = Person(self.graphdb, name="Cypher",
                            birthday=datetime.datetime(1972,10,16))
            smith = Person(self.graphdb, name="Agent Smith",
                           birthday=datetime.datetime(1996,01,01))
            cypher.friends.add(smith)
        friends = list(smith.friends)
        self.assertEqual(1, len(friends))
        self.assertEqual(cypher, friends[0])
        
    def test_access_relationship(self):
        with self.graphdb.transaction:
            cypher = Person(self.graphdb, name="Cypher",
                            birthday=datetime.datetime(1972,10,16))
            smith = Person(self.graphdb, name="Agent Smith",
                           birthday=datetime.datetime(1996,01,01))
            cypher.friends.add(smith, introduced=datetime.datetime(1999,01,01))
        friendships = list(smith.friends.rels)
        self.assertEqual(1, len(friendships))
        self.assertEqual(datetime.datetime(1999,01,01), friendships[0].introduced)
        

if __name__ == '__main__':
    unit_tests.unittest.main()

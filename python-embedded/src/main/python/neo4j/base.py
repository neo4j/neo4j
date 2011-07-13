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

"""Neo4j backend for Django.
"""

import neo4j

from django.db.backends import BaseDatabaseFeatures, BaseDatabaseOperations,\
    BaseDatabaseClient, BaseDatabaseIntrospection, BaseDatabaseValidation
from django.db.backends.creation import BaseDatabaseCreation

class DatabaseWrapper(object):
    def __init__(self, settings_dict, alias, *args, **kwargs):
        options = dict(settings_dict['OPTIONS']) # TODO: populate with more
        keys = dict((key.lower(),key) for key in settings_dict)
        for key in ('path', 'uri'):
            if key in keys:
                location = settings_dict[keys[key]]
                break
        else:
            location = options.pop('resourceUri',None)
        if location is None:
            raise ValueError("No database path specified")
        self.__graphdb = neo4j.GraphDatabase(location, **options)

        self.features = Neo4jFeatures()
        self.ops = Neo4jOperations()
        #self.client = None # TODO
        #self.creation = None # TODO
        #self.intospection = None # TODO
        self.validation = Neo4jValidation(self)

        self.settings_dict = settings_dict
        self.alias = alias

    def close(self):
        graphdb = self.__graphdb
        if graphdb is not None:
            graphdb.shutdown()
        self.__graphdb = None

class Neo4jFeatures(BaseDatabaseFeatures):
    
    def __init__(self):
      pass

class Neo4jOperations(BaseDatabaseOperations):
    # TODO
    pass

class Neo4jValidation(BaseDatabaseValidation):
    # TODO
    pass

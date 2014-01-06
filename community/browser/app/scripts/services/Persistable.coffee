###!
Copyright (c) 2002-2014 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

'use strict';

angular.module('neo4jApp.services')
  .factory 'Persistable', [
    'localStorageService'
    (localStorageService) ->
      class Persistable
        # Set all properties and generate an ID if missing
        constructor: (data = {})->
          if angular.isObject(data)
            angular.extend(@, data)
          @id ?= UUID.genV1().toString()

        #
        # Class methods
        #

        # Retrieve all items
        @fetch: ->
          persisted = try
            (localStorageService.get(@storageKey))
          catch
            null

          return [] unless angular.isArray(persisted)
          new @(p) for p in persisted

        # Save all items
        @save: (data) ->
          localStorageService.add(@storageKey, JSON.stringify(data))

      Persistable
  ]

###!
Copyright (c) 2002-2015 "Neo Technology,"
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
  .factory 'Document', [
    'Collection'
    'Persistable'
    (Collection, Persistable) ->
      class Document extends Persistable
        @storageKey = 'documents'

        constructor: (data) ->
          super data
          @name ?= 'Unnamed document'
          @folder ?= no

        toJSON: ->
          {@id, @name, @folder, @content}

      class Documents extends Collection
        create: (data) ->
          d = new Document(data)
          @add(d)
          @save()
          d
        klass: Document
        new: (args) -> new Document(args)
        remove: (doc) ->
          super
        destroy: (doc) ->
          @remove(doc)
          @save()

      new Documents(null, Document).fetch()
  ]

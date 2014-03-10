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
  .factory 'Document', [
    'Collection'
    'Persistable'
    'Settings'
    (Collection, Persistable, Settings) ->
      class Document extends Persistable
        @storageKey = 'documents'

        constructor: (data) ->
          super data
          @name ?= 'Unnamed document'
          @folder ?= no
          @metrics ?= {}

        update: (data, silent = no) ->
          super
          @metrics.updates = (@metrics.updates or 0) + 1 unless silent

        toJSON: ->
          angular.extend(super, {@folder, @content, @metrics})

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
          @save()

        update: (doc, args...) ->
          doc.update.apply(doc, args)
          @save()

        # separate method for this since we don't want to update timestamps
        updateMetrics: (doc, data) ->
          angular.extend(doc.metrics, data)
          @save()

      new Documents(null, Document).fetch()
  ]

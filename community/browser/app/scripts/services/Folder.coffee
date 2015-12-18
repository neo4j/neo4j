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
  .factory 'Folder', [
    'Collection'
    'Document'
    'Persistable'
    (Collection, Document, Persistable) ->
      class Folder extends Persistable
        @storageKey = 'folders'

        constructor: (data) ->
          @expanded = yes
          super data
          @name ?= 'Unnamed folder'

        toJSON: ->
          {@id, @name, @expanded}

      class Folders extends Collection
        create: (data) ->
          folder = new Folder(data)
          @add(folder)
          @save()
          folder
        expand: (folder) ->
          folder.expanded = !folder.expanded
          @save()
        klass: Folder
        new: (args) -> new Folder(args)
        destroy: (folder) ->
          documentsToRemove = Document.where(folder: folder.id)
          Document.remove(documentsToRemove)
          @remove(folder)
          @save()

      new Folders(null, Folder).fetch()
  ]

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
  .factory 'EventQueue', [
    '$log', 'Folder', 'Document'
    ($log, Folder, Document) ->
      class EventQueue
        trigger: (command, args...) ->
          switch command
            when "folder.create"
              Folder.create.apply(Folder, args)
            when "folder.remove"
              Folder.remove.apply(Folder, args)
            when "document.create"
              Document.create.apply(Document, args)
            when "document.remove"
              Document.remove.apply(Document, args)
              # also clear the document contents to cleanup editor content.
              args[k] = null for own k, v of args
            when "document.update"
              Document.update.apply(Document, args)
            when "document.update.metrics"
              Document.updateMetrics.apply(Document, args)
            else
              $log.error "No such event: #{command}"

      new EventQueue
  ]

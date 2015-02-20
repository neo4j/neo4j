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

'use strict'

angular.module('neo4jApp.controllers')
  .controller 'SidebarCtrl', [
    '$scope'
    'Document'
    'Editor'
    'Frame'
    'Folder'
    'GraphStyle'
    ($scope, Document, Editor, Frame, Folder, GraphStyle) ->
      ###*
       * Local methods
      ###
      scopeApply = (fn)->
        return ->
          fn.apply($scope, arguments)
          $scope.$apply()

      ###*
       * Scope methods
      ###
      $scope.removeFolder = (folder) ->
        return unless confirm("Are you sure you want to delete the folder?")
        Folder.destroy(folder)

      $scope.removeDocument = (doc) ->
        Document.destroy(doc)
        # also clear the document contents to cleanup editor content etc.
        doc[k] = null for own k, v of doc

      $scope.importDocument = (content, name) ->
        return GraphStyle.importGrass(content) if /\.grass$/.test name
        Document.create(content: content)

      $scope.playDocument = (content) ->
        Frame.create(input: content)

      ###*
       * Initialization
      ###

      # Handlers for drag n drop (for angular-ui-sortable)
      $scope.sortableOptions =
        connectWith: '.sortable'
        placeholder: 'sortable-placeholder'
        items: 'li'
        cursor: 'move'
        dropOnEmpty: yes

        stop: (e, ui) ->
          doc = ui.item.sortable.moved or ui.item.scope().document
          folder = if ui.item.folder? then ui.item.folder else doc.folder

          if ui.item.resort and ui.item.relocate
            doc.folder = folder
            doc.folder = false if doc.folder is 'root'

          # re-order the scripts according to order in sortable
          for folder in $scope.folders
            for doc in folder.documents
              Document.remove(doc)
              Document.add(doc)

          Document.save()

          return

        update: (e, ui) ->
          ui.item.resort = yes

        receive: (e, ui) ->
          ui.item.relocate = yes
          folder = angular.element(e.target).scope().folder
          ui.item.folder = if folder? then folder.id else false

      nestedFolderStructure = ->
        nested = for folder in Folder.all()
          documents = (doc for doc in Document.where({folder: folder.id}))
          folder.documents = documents
          folder

        noFolder = Folder.new(id: 'root')
        noFolder.documents = (doc for doc in Document.where({folder: false}))
        nested.push noFolder

        nested

      $scope.folders = nestedFolderStructure()

      $scope.$on 'localStorage:updated', ->
        $scope.folders = nestedFolderStructure()

      # Expose editor service to be able to play saved scripts
      $scope.editor = Editor

      $scope.substituteToken = (query, token) ->
        escapedToken = if token.match /^[A-Za-z][A-Za-z0-9_]*$/
          token
        else
          "`#{token}`"
        query.replace(/<token>/g, escapedToken)

      $scope.folderService = Folder
  ]

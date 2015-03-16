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
  .controller 'CypherResultCtrl', ['$rootScope', '$scope', ($rootScope, $scope) ->

    $scope.$watch 'frame.response', (resp) ->
      return unless resp
      # Initialise tab state from user selected if any
      $scope.tab = $rootScope.stickyTab
      # Otherwise try to detect the best mode
      if not $scope.tab?
        showGraph = resp.table.nodes.length
        $scope.tab = if showGraph then 'graph' else 'table'

    $scope.setActive = (tab) -> $rootScope.stickyTab = $scope.tab = tab
    $scope.isActive = (tab) -> tab is $scope.tab

    $scope.resultStatistics = (frame) ->
      if frame?.response
        stats = frame.response.table.stats
        fields = [
          {plural: 'constraints', singular: 'constraint', verb: 'added', field: 'constraints_added' }
          {plural: 'constraints', singular: 'constraint', verb: 'removed', field: 'constraints_removed' }
          {plural: 'indexes', singular: 'index', verb: 'added', field: 'indexes_added' }
          {plural: 'indexes', singular: 'index', verb: 'removed', field: 'indexes_removed' }
          {plural: 'labels', singular: 'label', verb: 'added', field: 'labels_added' }
          {plural: 'labels', singular: 'label', verb: 'removed', field: 'labels_removed' }
          {plural: 'nodes', singular: 'node', verb: 'created', field: 'nodes_created' }
          {plural: 'nodes', singular: 'node', verb: 'deleted', field: 'nodes_deleted' }
          {plural: 'properties', singular: 'property', verb: 'set', field: 'properties_set' }
          {plural: 'relationships', singular: 'relationship', verb: 'deleted', field: 'relationship_deleted' }
          {plural: 'relationships', singular: 'relationship', verb: 'created', field: 'relationships_created' }
        ]
        nonZeroFields = fields.filter((field) -> stats[field.field] > 0)
        messages = ("#{field.verb} #{stats[field.field]} #{if stats[field.field] is 1 then field.singular else field.plural}" for field in nonZeroFields)
        messages.push "returned #{frame.response.table.size} #{if frame.response.table.size is 1 then 'row' else 'rows'}"
        joinedMessages = messages.join(', ')
        "#{joinedMessages.substring(0, 1).toUpperCase()}#{joinedMessages.substring(1)}"
  ]

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

    $scope.displayInternalRelationships = $rootScope.stickyDisplayInternalRelationships ? true
    $scope.availableModes = []
    $scope.$watch 'frame.response', (resp) ->
      return unless resp
      # available combos:
      # - Graph + Table
      # - Table only
      $scope.availableModes = []
      $scope.availableModes.push('graph') if resp.table.nodes.length
      $scope.availableModes.push('table') if resp.table.size?
      $scope.availableModes.push('plan') if resp.table._response.plan

      # Initialise tab state from user selected if any
      $scope.tab = $rootScope.stickyTab

      # Always pre-select the plan tab if available
      if $scope.isAvailable('plan')
        $scope.tab = 'plan'

      # Otherwise try to detect the best mode
      if not $scope.tab?
        $scope.tab = $scope.availableModes[0] || 'table'

      # Override user tab selection if that mode doesn't exists
      $scope.tab = 'table' unless $scope.availableModes.indexOf($scope.tab) >= 0


    $scope.setActive = (tab) ->
      tab ?= if $scope.tab is 'graph' then 'table' else 'graph'
      $rootScope.stickyTab = $scope.tab = tab

    $scope.isActive = (tab) ->
      tab is $scope.tab

    $scope.isAvailable = (tab) ->
      tab in $scope.availableModes

    $scope.resultStatistics = (frame) ->
      if frame?.response
        updatesMessages = []
        if frame.response.table._response.columns.length
          updatesMessages = $scope.updatesStatistics frame
        rowsStatistics = $scope.returnedRowsStatistics frame
        messages = [].concat(updatesMessages, rowsStatistics)
        $scope.formatStatisticsOutput messages

    $scope.graphStatistics = (frame) ->
      if frame?.response
        graph = frame.response.graph
        plural = (collection, noun) ->
          "#{collection.length} #{noun}#{if collection.length is 1 then '' else 's'}"
        message = "Displaying #{plural(graph.nodes(), 'node')}, #{plural(graph.relationships(), 'relationship')}"
        internalRelationships = graph.relationships().filter((r) -> r.internal)
        if internalRelationships.length > 0
          message += " (completed with  #{plural(internalRelationships, 'additional relationship')})"
        message + '.'

    $scope.planStatistics = (frame) ->
      if frame?.response?.table?._response.plan?
        root = frame.response.table._response.plan.root
        collectHits = (operator) ->
          hits = operator.DbHits ? 0
          if operator.children
            for child in operator.children
              hits += collectHits(child)
          hits

        message = "Cypher version: #{root.version}, planner: #{root.planner}."
        if collectHits(root)
          message += " #{collectHits(root)} total db hits in #{frame.response.responseTime} ms."
        message

    $scope.formatStatisticsOutput = (messages) ->
      joinedMessages = messages.join(', ')
      "#{joinedMessages.substring(0, 1).toUpperCase()}#{joinedMessages.substring(1)}."

    $scope.returnedRowsStatistics = (frame) ->
      messages = []
      if frame?.response
        messages.push "returned #{frame.response.table.size} #{if frame.response.table.size is 1 then 'row' else 'rows'}"
        messages = getTimeString frame, messages, 'returnedRows'
        if (frame.response.table.size > frame.response.table.displayedSize)
          messages.push "displaying first #{frame.response.table.displayedSize} rows"
      messages

    $scope.updatesStatistics = (frame) ->
      messages = []
      if frame?.response
        stats = frame.response.table.stats
        nonZeroFields = $scope.getNonZeroStatisticsFields frame
        messages = ("#{field.verb} #{stats[field.field]} #{if stats[field.field] is 1 then field.singular else field.plural}" for field in nonZeroFields)
        messages = getTimeString frame, messages, 'updates'
      messages

    $scope.getNonZeroStatisticsFields = (frame) ->
      nonZeroFields = []
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
        nonZeroFields.push(field) for field in fields when stats[field.field] > 0
      nonZeroFields

    getTimeString = (frame, messages, context) ->
      timeMessage = " in #{frame.response.responseTime} ms"
      if context is 'updates'
        if messages.length and !frame.response.table._response.columns.length
          messages.push "statement executed"
          messages[messages.length - 1] += timeMessage

      if context is 'returnedRows'
        if frame.response.table._response.columns.length or (!frame.response.table._response.columns.length and !$scope.getNonZeroStatisticsFields(frame).length)
          messages[messages.length - 1] += timeMessage
      messages


    $scope.rerunCommand = (frame) ->
      $scope.$broadcast('reset.frame.views')
      frame.exec()

    # Listen for export events bubbling up the controller hierarchy
    # and forward them down to the child controller that has access to
    # the required SVG elements.
    $scope.$on('frame.export.graph.svg', ->
      $scope.$broadcast('export.graph.svg')
    )

    $scope.$on('frame.export.plan.svg', ->
      $scope.$broadcast('export.plan.svg')
    )

    $scope.$on('frame.export.graph.png', ->
      $scope.$broadcast('export.graph.png')
    )

    $scope.$on('frame.export.plan.png', ->
      $scope.$broadcast('export.plan.png')
    )

    $scope.toggleDisplayInternalRelationships = ->
      $scope.displayInternalRelationships = !$scope.displayInternalRelationships
  ]

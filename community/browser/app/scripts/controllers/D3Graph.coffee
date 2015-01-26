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

'use strict'

angular.module('neo4jApp.controllers')
  .controller('D3GraphCtrl', [
    '$attrs'
    '$element'
    '$parse'
    '$window'
    '$rootScope'
    '$scope'
    '$interval'
    'CircularLayout'
    'GraphExplorer'
    'GraphStyle'
    'CypherGraphModel',
    'exportService'
    ($attrs, $element, $parse, $window, $rootScope, $scope, $interval, CircularLayout, GraphExplorer, GraphStyle, CypherGraphModel, exportService) ->
      graphView = null

      measureSize = ->
        width: $element.width()
        height: $element.height()

      attributeHandlerFactory = (attribute) ->
        (item) ->
          if $attrs[attribute]
            exp = $parse($attrs[attribute])
            $scope.$apply(->exp($scope, {'$item': item }))

      itemMouseOver = attributeHandlerFactory('onItemMouseOver')
      itemMouseOut = attributeHandlerFactory('onItemMouseOut')
      nodeDragToggle = attributeHandlerFactory('onNodeDragToggle')
      onCanvasClicked = attributeHandlerFactory('onCanvasClicked')
      selectItem = attributeHandlerFactory('onItemClick')

      selectedItem = null

      toggleSelection = (d) =>
        if d is selectedItem
          d?.selected = no
          selectedItem = null
        else
          selectedItem?.selected = no
          d?.selected = yes
          selectedItem = d

        graphView.update()
        selectItem(selectedItem)

      $rootScope.$on 'layout.changed', (-> graphView?.resize())

      $scope.$on('export.graph.svg', ->
        svg = d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'svg'))
        svg.append('title').text('Neo4j Graph Visualization')
        svg.append('desc').text('Created using Neo4j (http://www.neo4j.com/)')
        d3.select($element.get(0)).selectAll('g.layer').each(->
          svg.node().appendChild($(this).clone().get(0))
        )
        svg.selectAll('.overlay, .ring').remove()
        svg.selectAll('text').attr('font-family', 'sans-serif')

        boundingBox = graphView.boundingBox()
        svg.attr('width', boundingBox.width)
        svg.attr('height', boundingBox.height)
        svg.attr('viewBox', [boundingBox.x, boundingBox.y, boundingBox.width, boundingBox.height].join(' '))

        exportService.download('graph.svg', 'image/svg+xml', new XMLSerializer().serializeToString(svg.node()))
        svg.remove()
      )

      @render = (initialGraph) ->
        graph = initialGraph
        return if graph.nodes().length is 0
        GraphExplorer.internalRelationships(graph.nodes())
        .then (result) =>
          graph.addRelationships(result.relationships.map(CypherGraphModel.convertRelationship(graph)))

          graphView = new neo.graphView($element[0], measureSize, graph, GraphStyle)

          $scope.style = GraphStyle.rules
          $scope.$watch 'style', (val) =>
            return unless val
            graphView.update()
          , true

          graphView
          .on('nodeClicked', (d) ->
              d.fixed = yes
              toggleSelection(d)
            )
          .on('nodeDblClicked', (d) ->
              return if d.expanded
              GraphExplorer.exploreNeighboursWithInternalRelationships(d, graph)
              .then(
                    # Success
                  () =>
                    linkDistance = 60
                    CircularLayout.layout(graph.nodes(), d, linkDistance)
                    d.expanded = yes
                    graphView.update()
                ,
                    # Error
                  (msg) ->
                    # Too many neighbours
                    alert(msg)
                )
              # New in Angular 1.1.5
              # https://github.com/angular/angular.js/issues/2371
              $rootScope.$apply() unless $rootScope.$$phase
            )
          .on('relationshipClicked', (d) ->
            toggleSelection(d)
          )
          .on('nodeMouseOver', itemMouseOver)
          .on('nodeMouseOut', itemMouseOut)
          .on('nodeDragToggle', nodeDragToggle)
          .on('relMouseOver', itemMouseOver)
          .on('relMouseOut', itemMouseOut)
          .on('canvasClicked', ->
            toggleSelection(null)
            onCanvasClicked()
          )
          .on('updated', ->
            $rootScope.$broadcast 'graph:changed', graph
          )

          emitStats = ->
            stats = graphView.collectStats()
            if stats.frameCount > 0
              $rootScope.$emit 'visualization:stats', stats

          $interval emitStats, 1000

          graphView.resize()
          $rootScope.$broadcast 'graph:changed', graph

      return @
  ])

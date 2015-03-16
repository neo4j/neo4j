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

clickcancel = ->
  cc = (selection) ->

    # euclidean distance
    dist = (a, b) ->
      Math.sqrt Math.pow(a[0] - b[0], 2), Math.pow(a[1] - b[1], 2)
    down = undefined
    tolerance = 5
    last = undefined
    wait = null
    selection.on "mousedown", ->
      d3.event.target.__data__.fixed = yes
      down = d3.mouse(document.body)
      last = +new Date()

    selection.on "mouseup", ->
      if dist(down, d3.mouse(document.body)) > tolerance
        return
      else
        if wait
          window.clearTimeout wait
          wait = null
          event.dblclick d3.event.target.__data__
        else
          wait = window.setTimeout(((e) ->
            ->
              event.click e.target.__data__
              wait = null
          )(d3.event), 250)

  event = d3.dispatch("click", "dblclick")
  d3.rebind cc, event, "on"

angular.module('neo4jApp.controllers')
  .controller('D3GraphCtrl', [
    '$element'
    '$window'
    '$rootScope'
    '$scope'
    'CircularLayout'
    'GraphExplorer'
    'GraphRenderer'
    'GraphStyle'
    'GraphGeometry'
    ($element, $window, $rootScope, $scope, CircularLayout, GraphExplorer, GraphRenderer, GraphStyle, GraphGeometry) ->

      linkDistance = 60

      el = d3.select($element[0])
      el.append('defs')
      graph = null

      selectedItem = null

      $scope.style = GraphStyle.rules
      $scope.$watch 'style', (val) =>
        return unless val
        @update()
      , true

      resize = ->
        height = $element.height()
        width  = $element.width()
        currentSize = force.size()
        if currentSize[0] != width or currentSize[1] != height
          force.size([width, height])
          force.start()

      selectItem = (item) ->
        $rootScope.selectedGraphItem = item
        $rootScope.$apply() unless $rootScope.$$phase

      onNodeDblClick = (d) =>
        return if d.expanded
        GraphExplorer.exploreNeighboursWithInternalRelationships(d, graph)
        .then(
          # Success
          () =>
            CircularLayout.layout(graph.nodes(), d, linkDistance)
            d.expanded = yes
            @update()
          ,
          # Error
          (msg) ->
            # Too many neighbours
            alert(msg)
        )
        # New in Angular 1.1.5
        # https://github.com/angular/angular.js/issues/2371
        $rootScope.$apply() unless $rootScope.$$phase

      onNodeClick = (d) =>
        d.fixed = yes
        toggleSelection(d)

      onRelationshipClick = (d) =>
        toggleSelection(d)

      toggleSelection = (d) =>
        if d is selectedItem
          d.selected = no
          selectedItem = null
        else
          selectedItem?.selected = no
          d.selected = yes
          selectedItem = d

        @update()
        selectItem(selectedItem)

      clickHandler = clickcancel()
      clickHandler.on 'click', onNodeClick
      clickHandler.on 'dblclick', onNodeDblClick

      accelerateLayout = (force, render) ->
        maxStepsPerTick = 100
        maxAnimationFramesPerSecond = 60
        maxComputeTime = 1000 / maxAnimationFramesPerSecond
        now = if angular.isDefined(window.performance) and angular.isFunction(window.performance.now)
          () -> window.performance.now()
        else
          () -> Date.now()

        d3Tick = force.tick
        force.tick = () ->
          startTick = now()
          step = maxStepsPerTick
          while step-- and now() - startTick < maxComputeTime
            if d3Tick()
              maxStepsPerTick = 2
              return true
          render()
          false

      render = ->
        GraphGeometry.onTick(graph)

        # Only translate nodeGroups, because this simplifies node renderers;
        # relationship renderers always take account of both node positions
        nodeGroups = el.selectAll("g.node")
        .attr("transform", (node) -> "translate(" + node.x + "," + node.y + ")")

        for renderer in GraphRenderer.nodeRenderers
          nodeGroups.call(renderer.onTick)

        relationshipGroups = el.selectAll("g.relationship")

        for renderer in GraphRenderer.relationshipRenderers
          relationshipGroups.call(renderer.onTick)

      force = d3.layout.force()
        .linkDistance(linkDistance)
        .charge(-1000)

      accelerateLayout(force, render)

      #
      # Public methods
      #
      @update = ->
        return unless graph
        nodes         = graph.nodes()
        relationships = graph.relationships()

        radius = nodes.length * linkDistance / (Math.PI * 2)
        center =
          x: $element.width() / 2
          y: $element.height() / 2
        CircularLayout.layout(nodes, center, radius)

        force
          .nodes(nodes)
          .links(relationships)
          .start()

        resize()
        $rootScope.$on 'layout.changed', resize

        layers = el.selectAll("g.layer").data(["relationships", "nodes"])

        layers.enter().append("g")
        .attr("class", (d) -> "layer " + d )

        relationshipGroups = el.select("g.layer.relationships")
        .selectAll("g.relationship").data(relationships, (d) -> d.id)

        relationshipGroups.enter().append("g")
        .attr("class", "relationship")
        .on("click", onRelationshipClick)

        GraphGeometry.onGraphChange(graph)

        for renderer in GraphRenderer.relationshipRenderers
          relationshipGroups.call(renderer.onGraphChange)

        relationshipGroups.exit().remove();

        nodeGroups = el.select("g.layer.nodes")
        .selectAll("g.node").data(nodes, (d) -> d.id)

        nodeGroups.enter().append("g")
        .attr("class", "node")
        .call(force.drag)
        .call(clickHandler)

        for renderer in GraphRenderer.nodeRenderers
          nodeGroups.call(renderer.onGraphChange);

        nodeGroups.exit().remove();

        # notify other graph observers
        $rootScope.$broadcast 'graph:changed', graph

      @render = (g) ->
        graph = g
        return if graph.nodes().length is 0
        GraphExplorer.internalRelationships(graph.nodes())
        .then (result) =>
          graph.addRelationships(result.relationships)
          @update()

      return @
  ])

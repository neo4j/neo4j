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

neo.layout = do ->
  _layout = {}

  _layout.force = ->
    _force = {}

    _force.init = (render) ->
      forceLayout = {}

      linkDistance = 45

      d3force = d3.layout.force()
      .linkDistance((relationship) -> relationship.source.radius + relationship.target.radius + linkDistance)
      .charge(-1000)

      newStatsBucket = ->
        bucket =
          layoutTime: 0
          layoutSteps: 0
        bucket

      currentStats = newStatsBucket()

      forceLayout.collectStats = ->
        latestStats = currentStats
        currentStats = newStatsBucket()
        latestStats

      accelerateLayout = ->
        maxStepsPerTick = 100
        maxAnimationFramesPerSecond = 60
        maxComputeTime = 1000 / maxAnimationFramesPerSecond
        now = if window.performance and window.performance.now
          () ->
            window.performance.now()
        else
          () ->
            Date.now()

        d3Tick = d3force.tick
        d3force.tick = ->
          startTick = now()
          step = maxStepsPerTick
          while step-- and now() - startTick < maxComputeTime
            startCalcs = now()
            currentStats.layoutSteps++

            neo.collision.avoidOverlap d3force.nodes()

            if d3Tick()
              maxStepsPerTick = 2
              return true
            currentStats.layoutTime += now() - startCalcs
          render()
          false

      accelerateLayout()

      oneRelationshipPerPairOfNodes = (graph) ->
        (pair.relationships[0] for pair in graph.groupedRelationships())

      forceLayout.update = (graph, size) ->

        nodes         = neo.utils.cloneArray(graph.nodes())
        relationships = oneRelationshipPerPairOfNodes(graph)

        radius = nodes.length * linkDistance / (Math.PI * 2)
        center =
          x: size[0] / 2
          y: size[1] / 2
        neo.utils.circularLayout(nodes, center, radius)

        d3force
        .nodes(nodes)
        .links(relationships)
        .size(size)
        .start()

      forceLayout.drag = d3force.drag
      forceLayout

    _force

  _layout
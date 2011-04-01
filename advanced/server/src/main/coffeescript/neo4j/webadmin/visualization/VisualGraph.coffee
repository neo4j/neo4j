###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/visualization/Renderer'
   'order!lib/jquery'
   'order!lib/arbor'
   'order!lib/arbor-graphics'
   'order!lib/arbor-tween'], 
  (Renderer) ->
  
    class VisualGraph

      constructor : (@server, width=800, height=400) ->
        @el = $("<canvas width='#{width}' height='#{height}'></canvas>")
        
        @groupCount = 0
        @visualizedGraph = {nodes:{}, edges:{}}
        
        @sys = arbor.ParticleSystem(1000, 600, 0.5, true, 30, 0.03)
        @stop()

        @sys.renderer = new Renderer(@el, this)
        @sys.renderer.bind "node:click", @nodeClicked
        @sys.screenPadding(20)

      getLabelFor : (visualNode) =>
        switch visualNode.data.type
          when "explored-node"
            return visualNode.data.neoNode.getSelf()
          when "unexplored-node"
            return "Unexplored"
          else
            return "Group"

      setNode : (node) =>
        @setNodes([node])

      setNodes : (nodes) =>

        @visualizedGraph = {nodes:{}, edges:{}}
        @addNodes nodes

      addNode : (node) =>
        @addNodes([node])

      addNodes : (nodes) =>
        # Short-hand references
        relMap = @visualizedGraph.edges
        nodeMap = @visualizedGraph.nodes

        fetchCountdown = nodes.length
        for node in nodes
          nodeMap[node.getSelf()] = { neoNode : node, type : "explored-node" }
          node.getRelationships().then (rels) =>
            for rel in rels
              
              nodeMap[rel.getStartNodeUrl()] ?= { neoUrl : rel.getStartNodeUrl(), type : "unexplored-node" }
              nodeMap[rel.getEndNodeUrl()] ?= { neoUrl : rel.getEndNodeUrl(), type : "unexplored-node" }

              relMap[rel.getStartNodeUrl()] ?= {}
              relMap[rel.getStartNodeUrl()][rel.getEndNodeUrl()] ?= { relationships : [] }
              relMap[rel.getStartNodeUrl()][rel.getEndNodeUrl()].relationships.push rel

            if (--fetchCountdown) == 0
              @visualizedGraph = { nodes:nodeMap, edges:relMap }
              # This deletes all current data not mentioned in our visualizedMap data structure
              # but retains the position of any data it recognizes.
              @sys.merge @visualizedGraph

      nodeClicked : (visualNode) =>
        if visualNode.data.type? and visualNode.data.type is "unexplored-node"
          @server.node(visualNode.data.neoUrl).then @addNode

      stop : () =>
        @sys.stop()

      start : () =>
        @sys.start()

      attach : (parent) =>
        @detach()
        $(parent).append(@el)
        @start()

      detach : () =>
        @stop()
        @el.detach()

)

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
   'neo4j/webadmin/visualization/NodeStyler'
   'neo4j/webadmin/visualization/RelationshipStyler'
   'order!lib/jquery'
   'order!lib/arbor'
   'order!lib/arbor-graphics'
   'order!lib/arbor-tween'], 
  (Renderer, NodeStyler, RelationshipStyler) ->
  
    class VisualGraph

      constructor : (@server, width=800, height=400, @groupingThreshold=10) ->
        @el = $("<canvas width='#{width}' height='#{height}'></canvas>")
        
        @nodeStyler = new NodeStyler()
        @relationshipStyler = new RelationshipStyler()

        @groupCount = 0
        @visualizedGraph = {nodes:{}, edges:{}}
        
        @sys = arbor.ParticleSystem(600, 100, 0.8, false, 30, 0.03)
        @stop()

        @sys.renderer = new Renderer(@el, @nodeStyler, @relationshipStyler)
        @sys.renderer.bind "node:click", @nodeClicked
        @sys.screenPadding(20)


      setNode : (node) =>
        @setNodes([node])

      setNodes : (nodes) =>
        # Nodes and edges are used by arbor.js, relationships is our own
        # map of relationship id -> relationship (edges are a map of node id -> node ids
        # plus relationship meta data)
        @visualizedGraph = {nodes:{}, edges:{}, relationships:{}}
        @addNodes nodes


      addNode : (node) =>
        @addNodes([node])

      addNodes : (nodes) =>
        # Short-hand references
        relToNodeMap = @visualizedGraph.edges
        nodeMap = @visualizedGraph.nodes
        relMap = @visualizedGraph.relationships

        fetchCountdown = nodes.length
        for node in nodes
          nodeMap[node.getSelf()] = { neoNode : node, type : "explored-node" }
          node.getRelationships().then (rels) =>
            for rel in rels
              nodeMap[rel.getStartNodeUrl()] ?= { neoUrl : rel.getStartNodeUrl(), type : "unexplored-node" }
              nodeMap[rel.getEndNodeUrl()] ?= { neoUrl : rel.getEndNodeUrl(), type : "unexplored-node" }

              if not relMap[rel.getSelf()]?
                relMap[rel.getSelf()] = rel
                relToNodeMap[rel.getStartNodeUrl()] ?= {}
                relToNodeMap[rel.getStartNodeUrl()][rel.getEndNodeUrl()] ?= { relationships : [], directed:true }
                relToNodeMap[rel.getStartNodeUrl()][rel.getEndNodeUrl()].relationships.push rel

            if (--fetchCountdown) == 0
              # This deletes all current data not mentioned in our visualizedMap data structure
              # but retains the position of any data it recognizes.
              @sys.merge @visualizedGraph

      nodeClicked : (visualNode) =>
        if visualNode.data.type? and visualNode.data.type is "unexplored-node"
          @server.node(visualNode.data.neoUrl).then @addNode


      getLabelFormatter : () =>
        @labelFormatter
      
      getNodeStyler : () =>
        @nodeStyler
      

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

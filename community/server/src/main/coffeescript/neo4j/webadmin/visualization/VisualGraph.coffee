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
   'neo4j/webadmin/visualization/VisualDataModel'
   'neo4j/webadmin/views/NodeFilterDialog'
   'order!lib/jquery'
   'order!lib/arbor'
   'order!lib/arbor-graphics'
   'order!lib/arbor-tween'], 
  (Renderer, NodeStyler, RelationshipStyler, VisualDataModel, NodeFilterDialog) ->
  
    class VisualGraph

      constructor : (@server, width=800, height=400, @groupingThreshold=10) ->
        @el = $("<canvas width='#{width}' height='#{height}'></canvas>")
        
        @labelProperties = []

        @nodeStyler = new NodeStyler()
        @relationshipStyler = new RelationshipStyler()

        @dataModel = new VisualDataModel()
        #@sys = arbor.ParticleSystem(600, 100, 0.8, false, 30, 0.03)
        @sys = arbor.ParticleSystem()
        @sys.parameters({
          repulsion:10, 
          stiffness:100, 
          friction:0.5,
          gravity:true,
          fps:30, 
          dt:0.015,
          precision:0.5
        })

        @stop()

        @sys.renderer = new Renderer(@el, @nodeStyler, @relationshipStyler)
        @sys.renderer.bind "node:click", @nodeClicked
        @sys.screenPadding(20)

        @steadStateWorker = setInterval(@steadyStateCheck, 1000)

      steadyStateCheck : () =>
        energy = @sys.energy()
        if energy?
          meanEnergy = energy.mean
          if  meanEnergy < 0.01 then @sys.stop()

      setNode : (node) =>
        @setNodes([node])

      setNodes : (nodes) =>
        @dataModel.clear()
        @addNodes nodes

      addNode : (node) =>
        @addNodes([node])

      addNodes : (nodes) =>
          
        fetchCountdown = nodes.length
        @sys.stop()
        for node in nodes
          do (node) =>
            relPromise = node.getRelationships()
            # Default depth 1 traversal, gets us all the end nodes of all relationships.
            relatedNodesPromise = node.traverse({})

            neo4j.Promise.join(relPromise, relatedNodesPromise).then (result) =>
              
              [rels, nodes] = result
              @dataModel.addNode node, rels, nodes
    
              if (--fetchCountdown) == 0
                @sys.merge @dataModel.getVisualGraph()
                @sys.start()
    
      nodeClicked : (visualNode) =>
        if visualNode.data.type?
          switch visualNode.data.type
            when "unexplored"
              @addNode visualNode.data.neoNode
            when "explored"
              @dataModel.unexplore visualNode.data.neoNode
              @sys.merge @dataModel.getVisualGraph()
            when "group"

              nodes = for url, groupedMeta of visualNode.data.group.grouped
                groupedMeta.node

              completeCallback = (filteredNodes, dialog) =>
                dialog.remove()
                @dataModel.ungroup filteredNodes
                @sys.merge @dataModel.getVisualGraph()

              dialog = new NodeFilterDialog { nodes : nodes, completeCallback : completeCallback, labelProperties : @labelProperties }
              dialog.show()


      setLabelProperties : (labelProps) ->
        @nodeStyler.setLabelProperties(labelProps)
        @labelProperties = labelProps
      
      getNodeStyler : () =>
        @nodeStyler
      
      reflow : () =>
        @sys.eachNode @floatNode
        @sys.parameters({gravity:true})
        @start()

      floatNode : (node, pt) =>
        node.fixed = false
        #node.p.x = (pt.x / 2)
        #node.p.y = (pt.y / 2)

      stop : () =>
        if @sys.renderer?
          @sys.renderer.stop()
        @sys.parameters({gravity:false})
        @sys.stop()

      start : () =>
        if @sys.renderer?
          @sys.renderer.start()
        @sys.start()

      attach : (parent) =>
        @detach()
        $(parent).prepend(@el)
        @start()

      detach : () =>
        @stop()
        @el.detach()

)

###
Copyright (c) 2002-2018 "Neo Technology,"
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

define(
  ['./Renderer'
   './RelationshipStyler'
   './VisualDataModel'
   './views/NodeFilterDialog'
   'feature!arbor'],
  (Renderer, RelationshipStyler, VisualDataModel, NodeFilterDialog, arbor) ->

    class VisualGraph

      constructor : (@server, @profile, width=800, height=400, @groupingThreshold=10) ->
        @el = $("<canvas width='#{width}' height='#{height}'></canvas>")

        @labelProperties = []

        @relationshipStyler = new RelationshipStyler()

        @dataModel = new VisualDataModel()

        if arbor.works
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

          @sys.renderer = new Renderer(@el, @relationshipStyler)
          @sys.renderer.bind "node:click", @nodeClicked
          @sys.renderer.bind "node:dropped", @nodeDropped
          @sys.screenPadding(20)

          @steadStateWorker = setInterval(@steadyStateCheck, 1000)
        else
          @el = $("<div class='missing' style='height:#{height}px'><div class='alert alert-error'><p><strong>Darn</strong>. I can see you have a beautiful graph. Sadly, I can't render that vision in this browser.</p></div></div>")

      steadyStateCheck : () =>
        energy = @sys.energy()
        if energy?
          meanEnergy = energy.mean
          if meanEnergy < 0.01 then @sys.stop()

      clear : () =>
        @dataModel.clear()
        @_synchronizeUiWithData()

      setNode : (node) =>
        @setNodes([node])

      setNodes : (nodes) =>
        @dataModel.clear()
        @addNodes nodes

      addNode : (node) =>
        @addNodes([node])

      addNodes : (nodes) =>

        fetchCountdown = nodes.length
        @stop()
        for node in nodes
          do (node) =>
            relPromise = node.getRelationships()
            # Default depth 1 traversal, gets us all the end nodes of all relationships.
            relatedNodesPromise = node.traverse({})

            neo4j.Promise.join(relPromise, relatedNodesPromise).then (result) =>

              [rels, relatedNodes] = result
              @dataModel.addNode node, rels, relatedNodes
              if (--fetchCountdown) == 0
                @_synchronizeUiWithData()

      nodeClicked : (visualNode, event) =>
        if visualNode.data.type?
          if event.button == 2
            1# TODO: right clicked, show context menu
          else
            switch visualNode.data.type
              when "unexplored"
                @addNode visualNode.data.neoNode
              when "explored"
                @dataModel.unexplore visualNode.data.neoNode
                @_synchronizeUiWithData()
              when "group"

                nodes = for url, groupedMeta of visualNode.data.group.grouped
                  groupedMeta.node

                completeCallback = (filteredNodes, dialog) =>
                  dialog.remove()
                  @dataModel.ungroup filteredNodes
                  @_synchronizeUiWithData()

                dialog = new NodeFilterDialog nodes, completeCallback
                dialog.show()


      nodeDropped : (dropped, target, event) ->
        neo4j.events.trigger("ui:node:dropped", {
            dropped:dropped.data.neoNode, target:target.data.neoNode,
            altKey:event.altKey, ctrlKey:event.ctrlKey, metaKey:event.metaKey,
            button:event.button,
        })

      reflow : () =>
        @sys.eachNode @floatNode
        @sys.parameters({gravity:true})
        @start()

      floatNode : (node, pt) =>
        node.fixed = false

      stop : () =>
        if arbor.works
          if @sys.renderer?
            @sys.renderer.stop()
          @sys.parameters({gravity:false})
          @sys.stop()

      start : () =>
        if arbor.works
          if @sys.renderer?
            @sys.renderer.start()
          @sys.start(true)

          # Force a redraw
          @sys.renderer.redraw()

      attach : (parent) =>
        @detach()
        $(parent).prepend(@el)
        @start()

      detach : () =>
        @stop()
        @el.detach()
        
      setProfile : (@profile) ->
        @_synchronizeUiWithData()
        
      _synchronizeUiWithData : () ->
        # Update styling
        for url, visualNode of @dataModel.getVisualGraph().nodes
          @profile.styleNode visualNode
        
        @_preloadIcons () =>
          @sys.merge @dataModel.getVisualGraph()
          @start()
        
      _preloadIcons : (done) ->
        @_images ?= {}
        
        @imagesLoading ?= 0
        hasGoneThroughAllNodes = false
        
        for url, visualNode of @dataModel.getVisualGraph().nodes
          style = visualNode.style
          if style.shapeStyle.shape is "icon"
            url = style.iconUrl
            if not @_images[url]?
              img = new Image()
              img.src = url
              @_images[url] = img
              
              @imagesLoading += 1
              img.onload = () =>
                @imagesLoading -= 1
                if @imagesLoading == 0 and hasGoneThroughAllNodes
                  done()
              
            style.icon = @_images[url]
        
        hasGoneThroughAllNodes = true
        if @imagesLoading == 0 then done()

)

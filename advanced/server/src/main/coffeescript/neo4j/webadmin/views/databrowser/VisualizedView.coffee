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
  ['neo4j/webadmin/data/ItemUrlResolver'
   'neo4j/webadmin/ui/LoadingSpinner'
   'neo4j/webadmin/templates/databrowser/visualization'
   'lib/raphael'
   'lib/dracula.graffle'
   'lib/dracula.graph'
   'lib/backbone'], 
  (ItemUrlResolver, LoadingSpinner, template) ->
  
    GROUP_IDS = 0

    class VisualizedView extends Backbone.View

      initialize : (options)->

        @server = options.server

        @urlResolver = new ItemUrlResolver(@server)

        @dataModel = options.dataModel
        @dataModel.bind("change", @render)

        @nodeMap = {}
        @groupMap = {}

      render : =>
        if @dataModel.get("type") is "node"
          @baseNode = @dataModel.getData().getItem()
        else 
          return this
        $(@el).html(template())

        width = $(document).width() - 40;
        height = $(document).height() - 120;

        @g = new Graph()
        
        @addNode(@baseNode)
        @loadRelationships(@baseNode)

        @layout = new Graph.Layout.Spring(@g)
        @renderer = new Graph.Renderer.Raphael('visualization', @g, width, height)

        return this

      redrawVisualization : =>
        @layout.layout()
        @renderer.draw()

      hasNode : (nodeUrl) =>
         if @nodeMap[nodeUrl]
           return true
         else
           return false
 
      addNode : (node) =>
        @nodeMap[node.getSelf()] = 
          neoNode    : node
          visualNode : @g.addNode(node.getSelf(), { node : node, url:node.getSelf(), explored:true, render:@nodeRenderer })

      addUnexploredNode : (nodeUrl) =>
        @nodeMap[nodeUrl] = 
          neoNode    : null
          visualNode : @g.addNode(nodeUrl, { node : null, url:nodeUrl, explored:false, render:@unexploredNodeRenderer})

      addRelationships : (rels, node) =>
        groups = {}
        for rel in rels
          if not groups[rel.getType()]
            groups[rel.getType()] = { type:rel.getType(), size:0, relationships:[] }

          groups[rel.getType()].size++
          groups[rel.getType()].relationships.push(rel)

        for trype, group of groups

          if group.size > 5
            @addGroup(group, node)
          else
            @addExplodedGroup(group, node)
          
        @redrawVisualization()

      addGroup : (group, node) =>

        id = "group-" + GROUP_IDS++
        @groupMap[id] = 
          group : group
          visualNode : @g.addNode(id, { id:id, size:group.size, render:@groupRenderer })

        @g.addEdge(node.getSelf(), id, { label : group.type })

      addExplodedGroup : (group) =>
        
        for rel in group.relationships
          if @hasNode(rel.getEndNodeUrl()) == false
            @addUnexploredNode(rel.getEndNodeUrl())

          if @hasNode(rel.getStartNodeUrl()) == false
            @addUnexploredNode(rel.getStartNodeUrl())
          
          @g.addEdge(rel.getStartNodeUrl(), rel.getEndNodeUrl(), { label : rel.getType(), directed : true })


      nodeRenderer : (r, node) =>
        circle = r.circle(0, 0, 10).attr({fill: "#ffffff", stroke: "#333333", "stroke-width": 2})

        if node.node.hasProperty("name")        
          label = r.text(0, 0, node.node.getProperty("name"))
        else
          label = r.text(0, 0, @urlResolver.extractNodeId(node.url))
        
        clickHandler = (ev) =>
          @nodeClicked(node, circle)

        circle.click(clickHandler)
        label.click(clickHandler)

        shape = r.set().
          push(circle).
          push(label)
        return shape

      unexploredNodeRenderer : (r, node) =>
        circle = r.circle(0, 0, 10).attr({fill: "#ffffff", stroke: "#dddddd", "stroke-width": 2})

        label = r.text(0, 0, @urlResolver.extractNodeId(node.url))
          
        clickHandler = (ev) =>
          @nodeClicked(node, circle)
        circle.click(clickHandler)
        label.click(clickHandler)

        shape = r.set().
          push(circle).
          push(label)
        return shape

      groupRenderer : (r, group) =>
        circle = r.circle(0, 0, 6).attr({fill: "#eeeeee", stroke: "#dddddd", "stroke-width": 2})
        
        label = r.text(0, 0, group.size)
          
        clickHandler = (ev) =>
          @groupClicked(group, circle)
        circle.click(clickHandler)
        label.click(clickHandler)

        shape = r.set().
          push(circle).
          push(label)
        return shape
      
      nodeClicked : (nodeMeta, circle) =>
        nodeMeta = @nodeMap[nodeMeta.url].visualNode

        if nodeMeta.explored == false
          circle.attr({fill: "#ffffff", stroke: "#333333", "stroke-width": 2})
          nodeMeta.explored = true
          @server.node(nodeMeta.url).then (node) =>
            @addNode(node)
            @loadRelationships(node)

      groupClicked : (groupMeta, circle) =>
        group = @groupMap[groupMeta.id].group
        visualNode = @groupMap[groupMeta.id].visualNode
        @showLoader()
    
        ungroup = ()=>
          @removeVisualNode(visualNode)
          @addExplodedGroup(group)
          @redrawVisualization()
          @hideLoader()

        setTimeout( ungroup, 1 )

      loadRelationships : (node) =>
        @showLoader()
        node.getRelationships().then (rels) =>
          @addRelationships(rels, node)
          @hideLoader()

      removeVisualNode : (visualNode, id) =>
        visualNode.hide()
        for edge in visualNode.edges
          edge.connection.label.hide()
          edge.hide()
        @g.removeNode(visualNode.id)

      showLoader : =>
        @hideLoader()        
        @loader = new LoadingSpinner($(".workarea"))
        @loader.show()

      hideLoader : =>
        if @loader?
          @loader.destroy()


      remove : =>
        @dataModel.unbind("change", @render)
        super()

)

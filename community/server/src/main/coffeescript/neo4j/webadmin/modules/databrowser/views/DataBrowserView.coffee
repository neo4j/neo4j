###
Copyright (c) 2002-2011 "Neo Technology,"
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
  ['neo4j/webadmin/utils/ItemUrlResolver',
   './TabularView',
   './VisualizedView',
   './CreateRelationshipDialog',
   'ribcage/View',
   './base','lib/backbone'], 
  (ItemUrlResolver, TabularView, VisualizedView, CreateRelationshipDialog, View, template) ->

    class DataBrowserView extends View
      
      template : template

      events : 
        "keypress #data-console" : "consoleKeyPressed"
        "click #data-create-node" : "createNode"
        "click #data-create-relationship" : "createRelationship"
        "click #data-switch-view" : "switchView"
        "click #data-execute-console" : "search"

      initialize : (options)->
        @dataModel = options.dataModel
        @appState = options.state
        @server = options.state.getServer()

        @urlResolver = new ItemUrlResolver(@server)
        
        @dataModel.bind("change:query", @queryChanged)
        @switchToTabularView()

      render : =>
        $(@el).html @template( 
          query : @dataModel.getQuery()
          viewType : @viewType
          dataType : @dataModel.getDataType() )
        @renderDataView()

      renderDataView : =>
        @dataView.attach($("#data-area", @el).empty())
        @dataView.render()
        return this

      queryChanged : =>
        $("#data-console",@el).val(@dataModel.getQuery())

      search : (ev) =>
        @dataModel.setQuery( $("#data-console",@el).val(), false, { force:true, silent:true})
        @dataModel.trigger("change:query")

      createNode : =>
        @server.node({}).then (node) =>
          id = @urlResolver.extractNodeId(node.getSelf())
          @dataModel.setData( node, true ) 
          @dataModel.setQuery( id, true) 

      createRelationship : =>
        if @createRelationshipDialog?
          @hideCreateRelationshipDialog()
        else
          button = $("#data-create-relationship")
          button.addClass("selected")
          @createRelationshipDialog = new CreateRelationshipDialog(
            baseElement : button
            dataModel : @dataModel
            server : @server
            closeCallback : @hideCreateRelationshipDialog)

      hideCreateRelationshipDialog : =>
        if @createRelationshipDialog?
          @createRelationshipDialog.remove()
          delete(@createRelationshipDialog)
          $("#data-create-relationship").removeClass("selected")

      consoleKeyPressed : (ev) =>
        if ev.which is 13# and ev.ctrlKey # ctrl + enter
          ev.stopPropagation()
          @search()

      switchView : (ev) =>
        if @viewType == "visualized"
          $(ev.target).removeClass("tabular")
          @switchToTabularView()
        else
          $(ev.target).addClass("tabular")
          @switchToVisualizedView()
        
        @renderDataView()
          

      switchToVisualizedView : =>
        
        if @dataView?
          @dataView.detach()
        
        @visualizedView ?= new VisualizedView(dataModel:@dataModel, appState:@appState, server:@server)
        @viewType = "visualized"
        @dataView = @visualizedView 

      switchToTabularView : =>
        if @dataView?
          @dataView.detach()
      
        @tabularView ?= new TabularView(dataModel:@dataModel, appState:@appState, server:@server)
        @viewType = "tabular"
        @dataView = @tabularView

      unbind : ->
        @dataModel.unbind("change:query", @queryChanged)
        
      detach : ->
        @unbind()
        @hideCreateRelationshipDialog()
        if @dataView?
          @dataView.detach()
        super()

      remove : =>
        @unbind()
        @hideCreateRelationshipDialog()
        @dataView.remove()

)

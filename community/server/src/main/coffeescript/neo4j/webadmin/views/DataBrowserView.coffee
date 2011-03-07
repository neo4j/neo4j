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
  ['neo4j/webadmin/data/Search',
   'neo4j/webadmin/data/ItemUrlResolver',
   './databrowser/SimpleView',
   'neo4j/webadmin/templates/data/base','lib/backbone'], 
  (Search, ItemUrlResolver, SimpleView, template) ->

    class DataBrowserView extends Backbone.View
      
      template : template

      events : 
        "keyup #data-console" : "search"
        "click #data-create-node" : "createNode"

      initialize : (options)->
        @dataModel = options.dataModel
        @server = options.state.getServer()
        @urlResolver = new ItemUrlResolver(@server)
        @dataView = new SimpleView(dataModel:options.dataModel)

      render : ->
        $(@el).html(@template())
        $("#data-area", @el).append @dataView.el 
        return this

      search : (ev) =>
        @dataModel.setQuery( $("#data-console",@el).val() )

      createNode : =>
        @server.node({}).then (node) =>
          id = @urlResolver.extractNodeId(node.getSelf())
          @dataModel.setData( node, true, {silent:true} ) 
          @dataModel.setQuery( id, true) 
)

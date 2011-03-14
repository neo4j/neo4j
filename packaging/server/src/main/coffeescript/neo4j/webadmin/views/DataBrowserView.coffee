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
   'neo4j/webadmin/security/HtmlEscaper',
   './databrowser/SimpleView',
   './databrowser/CreateRelationshipDialog',
   'neo4j/webadmin/templates/databrowser/base','lib/backbone'], 
  (Search, ItemUrlResolver, HtmlEscaper, SimpleView, CreateRelationshipDialog, template) ->

    class DataBrowserView extends Backbone.View
      
      template : template

      events : 
        "keyup #data-console" : "search"
        "click #data-create-node" : "createNode"
        "click #data-create-relationship" : "createRelationship"

      initialize : (options)->
        @dataModel = options.dataModel
        @server = options.state.getServer()

        @htmlEscaper = new HtmlEscaper

        @urlResolver = new ItemUrlResolver(@server)
        @dataView = new SimpleView(dataModel:@dataModel)

        @dataModel.bind("change:query", @queryChanged)

      render : =>
        $(@el).html(@template( query : @htmlEscaper.escape(@dataModel.getQuery()) ))
        $("#data-area", @el).append @dataView.render().el
        return this

      queryChanged : =>
        $("#data-console",@el).val(@dataModel.getQuery())

      search : (ev) =>
        @dataModel.setQuery( $("#data-console",@el).val() )

      createNode : =>
        @server.node({}).then (node) =>
          id = @urlResolver.extractNodeId(node.getSelf())
          @dataModel.setData( node, true, {silent:true} ) 
          @dataModel.setQuery( id, true) 

      createRelationship : =>
        button = $("#data-create-relationship")
        if @createRelationshipDialog?
          @createRelationshipDialog.remove()
          delete(@createRelationshipDialog)
          button.removeClass("selected")
        else
          button.addClass("selected")
          @createRelationshipDialog = new CreateRelationshipDialog(button)
)

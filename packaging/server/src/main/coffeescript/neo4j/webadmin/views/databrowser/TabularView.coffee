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
  ['./NodeView',
   './RelationshipView',
   './ListView',
   'neo4j/webadmin/views/View',
   'neo4j/webadmin/templates/databrowser/notfound',
   'lib/backbone'], 
  (NodeView, RelationshipView, ListView, View, notFoundTemplate) ->
  
    class SimpleView extends View

      initialize : (options)->
        
        @nodeView = new NodeView
        @relationshipView = new RelationshipView
        @listView = new ListView

        @dataModel = options.dataModel
        @dataModel.bind("change", @render)

      render : =>
        type = @dataModel.get("type")
        switch type
          when "node"
            view = @nodeView
          when "relationship"
            view = @relationshipView
          when "set"
            view = @listView
          else
            $(@el).html(notFoundTemplate())
            return this
        view.setDataModel(@dataModel)
        $(@el).html(view.render().el)
        view.delegateEvents()
        return this
      
      remove : =>
        @dataModel.unbind("change", @render)
        @nodeView.remove()
        @relationshipView.remove()
        @listView.remove()
        super()


)

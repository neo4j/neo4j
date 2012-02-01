###
  Copyright (c) 2002-2012 "Neo Technology,"
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
  ['./NodeView',
   './RelationshipView',
   './RelationshipListView',
   './NodeListView',
   'ribcage/View',
   './notfound',
   'lib/backbone'], 
  (NodeView, RelationshipView, RelationshipListView, NodeListView, View, notFoundTemplate) ->
  
    class SimpleView extends View

      initialize : (options)->
        
        @nodeView = new NodeView
        @relationshipView = new RelationshipView
        @relationshipListView = new RelationshipListView
        @nodeListView = new NodeListView

        @dataModel = options.dataModel
        @dataModel.bind("change:data", @render)

      render : =>
        type = @dataModel.get("type")
        switch type
          when "node"
            view = @nodeView
          when "nodeList"
            view = @nodeListView
          when "relationship"
            view = @relationshipView
          when "relationshipList"
            view = @relationshipListView
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
        @nodeListView.remove()
        @relationshipView.remove()
        @relationshipListView.remove()
        super()


)

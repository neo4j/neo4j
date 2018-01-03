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
  ['./NodeView'
   './RelationshipView'
   './RelationshipListView'
   './NodeListView'
   './CypherResultView'
   'neo4j/webadmin/modules/databrowser/models/DataBrowserState'
   'ribcage/View'
   './notfound'
   'lib/amd/jQuery'], 
  (NodeView, RelationshipView, RelationshipListView, NodeListView, CypherResultView, DataBrowserState, View, notFoundTemplate, $) ->
  
    State = DataBrowserState.State

    class SimpleView extends View

      initialize : (options)->
        
        @dataModel = options.dataModel
        
        @nodeView =             new NodeView         dataModel : @dataModel
        @relationshipView =     new RelationshipView dataModel : @dataModel
        @relationshipListView = new RelationshipListView
        @nodeListView =         new NodeListView
        @cypherResultView =     new CypherResultView

        @dataModel.bind("change:data", @render)

      render : =>
        state = @dataModel.getState()
        switch state
          when State.SINGLE_NODE
            view = @nodeView
          when State.NODE_LIST
            view = @nodeListView
          when State.SINGLE_RELATIONSHIP
            view = @relationshipView
          when State.RELATIONSHIP_LIST
            view = @relationshipListView
          when State.CYPHER_RESULT
            view = @cypherResultView
          when State.EMPTY
            $(@el).html(notFoundTemplate())
            return this
          when State.NOT_EXECUTED
            return this
          when State.ERROR
            return this

        view.setData(@dataModel.getData())
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

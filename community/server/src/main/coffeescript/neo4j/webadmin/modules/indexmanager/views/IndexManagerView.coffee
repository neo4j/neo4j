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
  ['./base',
   './index',
   './IndexView'
   'ribcage/View',
   'lib/amd/jQuery'], 
  (template, indexTemplate, IndexView, View, $) ->

    class IndexManagerView extends View
      
      template : template
      
      events : 
        "click .create-node-index" : "createNodeIndex"
        "click .create-rel-index" : "createRelationshipIndex"
     
      initialize : (opts) =>
        @appState = opts.state
        @server = @appState.getServer()
        @idxMgr = opts.idxMgr
        @idxMgr.bind("change", @renderIndexList)

      render : =>
        $(@el).html(template())
        @renderIndexList()        
        return this
        
      renderIndexList : =>
        nodeIndexList = $("#node-indexes", @el).empty()
        for index in @idxMgr.get "nodeIndexes" 
          nodeIndexList.append( new IndexView({index : index, idxMgr : @idxMgr, type:IndexView.prototype.NODE_INDEX_TYPE}).render().el )
        
        relIndexList = $("#rel-indexes", @el).empty()
        for index in @idxMgr.get "relationshipIndexes" 
          relIndexList.append( new IndexView({index : index, idxMgr : @idxMgr, type:IndexView.prototype.REL_INDEX_TYPE}).render().el )
      
      createNodeIndex : => 
        @idxMgr.createNodeIndex({name : $("#create-node-index-name").val()})

      createRelationshipIndex : => 
        @idxMgr.createRelationshipIndex({name : $("#create-rel-index-name").val()})
)

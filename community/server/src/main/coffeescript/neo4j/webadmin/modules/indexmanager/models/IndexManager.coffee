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

define ['ribcage/Model'], (Model) ->
  
  class IndexManager extends Model
    
    defaults : 
      nodeIndexes : []
      relationshipIndexes : []
    
    initialize : (opts) =>
      @server = opts.server
      @server.index.getAllNodeIndexes().then (res) => @set {"nodeIndexes" : res}
      @server.index.getAllRelationshipIndexes().then (res) => @set {"relationshipIndexes" : res}
    
    createNodeIndex: (opts) =>
      name = opts.name
      if @_hasNodeIndex name then return
      @server.index.createNodeIndex(name).then (index) => 
        @get("nodeIndexes").push index
        @trigger "change"

    createRelationshipIndex : (opts) =>
      name = opts.name
      if @_hasRelationshipIndex name then return
      @server.index.createRelationshipIndex(name).then (index) => 
        @get("relationshipIndexes").push index
        @trigger "change"

    deleteNodeIndex: (opts) =>
      name = opts.name
      @server.index.removeNodeIndex(name).then () =>
        @set "nodeIndexes" : @_removeIndexFromList(@get("nodeIndexes"), name )
        @trigger "change"

    deleteRelationshipIndex: (opts) =>
      name = opts.name
      @server.index.removeRelationshipIndex(name).then () =>
        @set "relationshipIndexes" : @_removeIndexFromList( @get("relationshipIndexes"), name )
        @trigger "change"

    _hasRelationshipIndex : (name) =>
      for idx in @get "relationshipIndexes"
        if idx.name == name then return true
      return false

    _hasNodeIndex : (name) =>
      for idx in @get "nodeIndexes"
        if idx.name == name then return true
      return false

    _removeIndexFromList : (idxs, name) ->
      for i in [idxs.length-1..0]
        if idxs[i].name == name
          idxs.splice(i,1)
          break
      return idxs


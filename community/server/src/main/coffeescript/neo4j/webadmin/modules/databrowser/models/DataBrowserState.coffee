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
  ['./NodeProxy'
   './NodeList'
   './RelationshipProxy'
   './RelationshipList'
   'ribcage/Model'], 
  (NodeProxy, NodeList, RelationshipProxy, RelationshipList, Model) ->
  
    class DataBrowserState extends Model
      
      defaults :
        type : null
        data : null
        query : null
        queryOutOfSyncWithData : true

      initialize : (options) =>
        @server = options.server

      getQuery : =>
        @get "query"

      getData : =>
        @get "data"
      
      getDataType : =>
        @get "type"

      dataIsSingleNode : () =>
        return @get("type") == "node"
     
      dataIsSingleRelationship : () =>
        return @get("type") == "relationship"

      setQuery : (val, isForCurrentData=false, opts={}) =>
        if @get("query") != val or opts.force is true
          @set {"queryOutOfSyncWithData": not isForCurrentData }, opts
          @set {"query" : val }, opts

      setData : (result, basedOnCurrentQuery=true, opts={}) =>
        @set({"data":result, "queryOutOfSyncWithData" : not basedOnCurrentQuery }, {silent:true})

        if result instanceof neo4j.models.Node
          return @set({type:"node","data":new NodeProxy(result)}, opts)

        else if result instanceof neo4j.models.Relationship
          return @set({type:"relationship","data":new RelationshipProxy(result)}, opts)

        else if _(result).isArray() and result.length > 0 

          if result.length is 1 # If only showing one item, show it in single-item view
            return @setData(result[0], basedOnCurrentQuery, opts)
          else
            if result[0] instanceof neo4j.models.Relationship
              return @set({type:"relationshipList", "data":new RelationshipList(result)}, opts)
            else if result[0] instanceof neo4j.models.Node
              return @set({type:"nodeList", "data":new NodeList(result)}, opts)
        else if result instanceof neo4j.cypher.QueryResult and result.size() > 0
          @set({type:"cypher"})
          return @trigger "change:data"

        @set({type:"not-found", "data":null}, opts)

)

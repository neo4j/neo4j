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
  ['lib/backbone'], 
  () ->
  
    class DataBrowserState extends Backbone.Model
      
      defaults :
        type : null
        data : null
        query : "0"
        queryOutOfSyncWithData : true

      initialize : (options) =>
        @server = options.server

      getEscapedQuery : =>
        encodeURIComponent(@get "query")

      setQuery : (val, isForCurrentData=false, opts={}) =>
        if @get("query") != val
          @set {"queryOutOfSyncWithData": not isForCurrentData }, opts
          @set {"query" : val }, opts

      setData : (result, basedOnCurrentQuery=true, opts={}) =>
        @set({"data":result, "queryOutOfSyncWithData" : not basedOnCurrentQuery }, {silent:true})

        if result instanceof neo4j.models.Node
          @set({type:"node"}, opts)
        else if result instanceof neo4j.models.Relationship
          @set({type:"relationship"}, opts)
        else if _(result).isArray()
          @set({type:"list"}, opts)
        else
          @set({"data":null, type:"not-found"}, opts)

)

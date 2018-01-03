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
  ['neo4j/webadmin/modules/databrowser/search/QueuedSearch',
   './NodeProxy'
   './NodeList'
   './RelationshipProxy'
   './RelationshipList'
   'ribcage/time/Timer'
   'ribcage/Model'], 
  (QueuedSearch, NodeProxy, NodeList, RelationshipProxy, RelationshipList, Timer, Model) ->
  
    class DataBrowserState extends Model

      @State :
        ERROR               : -1
        EMPTY               : 0
        NOT_EXECUTED        : 1
        SINGLE_NODE         : 2
        SINGLE_RELATIONSHIP : 3
        NODE_LIST           : 4
        RELATIONSHIP_LIST   : 5
        CYPHER_RESULT       : 6

      class @QueryMetaData extends Model
        
        defaults : 
          executionTime : 0
          numberOfRows  : 0

        getExecutionTime : -> @get "executionTime"
        getNumberOfRows  : -> @get "numberOfRows"

        setExecutionTime : (t) -> @set executionTime : t
        setNumberOfRows  : (n) -> @set numberOfRows  : n
        
      
      defaults :
        data : null
        query : "START root=node(0) // Start with the reference node\n" +
                "RETURN root        // and return it.\n" +
                "\n" +
                "// Hit CTRL+ENTER to execute"
        queryOutOfSyncWithData : true
        state : DataBrowserState.State.NOT_EXECUTED
        querymeta : new DataBrowserState.QueryMetaData()

      initialize : (options) =>
        @searcher = new QueuedSearch(options.server)
        @_executionTimer = new Timer

      getQuery : =>
        @get "query"

      getData : =>
        @get "data"
      
      getState : =>
        @get "state"
      
      getQueryMetadata : =>
        @get "querymeta"

      setQuery : (val, isForCurrentData=false, opts={}) =>
        if @getQuery() != val or opts.force is true
          if not isForCurrentData
            state = DataBrowserState.State.NOT_EXECUTED
          else
            state = @getState()
          
          @set {"query":val, "state":state, "queryOutOfSyncWithData": not isForCurrentData }, opts
          if state is DataBrowserState.State.NOT_EXECUTED
            @set {"data":null}, opts

      executeCurrentQuery : =>
        @_executionTimer.start()
        @searcher.exec(@getQuery()).then(@setData,@setData)

      setData : (result, basedOnCurrentQuery=true, opts={}) =>
        @_executionTimer.stop()
  
        executionTime = @_executionTimer.getTimePassed()
        originalState = @getState()

        # These to be determined below
        state = null
        data = null
        numberOfRows = null

        if result instanceof neo4j.models.Node
          state = DataBrowserState.State.SINGLE_NODE
          data = new NodeProxy(result, @_reportError)

        else if result instanceof neo4j.models.Relationship
          state = DataBrowserState.State.SINGLE_RELATIONSHIP
          data = new RelationshipProxy(result, @_reportError)

        else if _(result).isArray() and result.length is 0 
          state = DataBrowserState.State.EMPTY

        else if _(result).isArray() and result.length is 1
          # If only showing one item, show it in single-item view
          return @setData(result[0], basedOnCurrentQuery, opts)

        else if _(result).isArray()
          if result[0] instanceof neo4j.models.Relationship
            state = DataBrowserState.State.RELATIONSHIP_LIST
            data = new RelationshipList(result)

          else if result[0] instanceof neo4j.models.Node
            state = DataBrowserState.State.NODE_LIST
            data = new NodeList(result)
        
        else if result instanceof neo4j.cypher.QueryResult and result.size() is 0
          state = DataBrowserState.State.EMPTY

        else if result instanceof neo4j.cypher.QueryResult
          state = DataBrowserState.State.CYPHER_RESULT
          data = result
      
        else if result instanceof neo4j.exceptions.NotFoundException
          state = DataBrowserState.State.EMPTY

        else
          state = DataBrowserState.State.ERROR
          data = result

        # Query meta data
        if state isnt DataBrowserState.State.ERROR
          @_updateQueryMetaData(data,executionTime)

        @set({"state":state, "data":data, "queryOutOfSyncWithData" : not basedOnCurrentQuery }, {silent:true})
        if not opts.silent
          @trigger "change:data"
          @trigger "change:state" if originalState != state
      
      # Callback that gets passed to smart objects that we create that may generate errors
      # that they don't know how to recover from. They use this to notify the data browser that
      # something went wrong, and that we should tell the user about it. 
      _reportError : (error) =>
        # Currently just delegates to setData, which will move this object to the ERROR state
        # if it does not understand the data we give it.
        @setData(error)
      
      _updateQueryMetaData : (data, executionTime) ->
        if data?
          if data instanceof neo4j.cypher.QueryResult
            numberOfRows = data.data.length
          else
            numberOfRows = if data.length? then data.length else 1
        else
          numberOfRows = 0
        meta = @getQueryMetadata()
        meta.setNumberOfRows(numberOfRows)
        meta.setExecutionTime(executionTime)
        
        @trigger "change:querymeta"

)

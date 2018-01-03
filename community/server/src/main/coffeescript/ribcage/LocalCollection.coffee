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
  ['lib/amd/Backbone'], 
  (Backbone) ->
    ###
    A local collection keeps track of ids locally, and adds the 
    methods #setFetchMethod and #setSaveMethod to the Collection
    API. These allow persistence strategies that do not depend
    on global variables.
    ###
    class LocalCollection extends Backbone.Collection
      
      constructor : (items) ->
        if not _(items).isArray()
          items = []
        # Used for simple id generation
        @_idCounter = 0
        for p in items
          @_idCounter = p.id if p.id and p.id > @_idCounter
        super(items)
      
      add : (items, opts) ->
        if not _(items).isArray()
          items = [items]
          
        processed = []
        for item in items
          if not item.id?
            item.id = ++@_idCounter
          
          if item not instanceof Backbone.Model
            try
              item = @deserializeItem item
            catch e
              #neo4j.log "Unable to deserialize model, error was: ", e
              continue
            
          modelFetch = () =>
            @fetch(item.id)
            
          item.setSaveMethod @save if item.setSaveMethod? 
          item.setFetchMethod modelFetch if item.setFetchMethod? 
          processed.push item
        super(processed, opts)
        
      fetch : (id=null) =>
        jsonItems = @_fetch()
        if id == null
          @update(jsonItems)
        else
          for item in jsonItems
            if id == item.id
              return item
          
      update : (items) ->
        for jsonItem in jsonItems
          if (item = @get jsonItem.id) != null
            item.clear silent:true
            item.set jsonItem
          else
            @add jsonItem
                 
      save : () =>
        @_save this
        
      deserializeItem : (itemJSON) ->
        if @model?
          return new @model(itemJSON)
        return itemJSON
        
      setFetchMethod : (@_fetch) ->
      setSaveMethod : (@_save) ->
      
)

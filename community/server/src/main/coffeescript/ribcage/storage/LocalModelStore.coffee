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
  ['lib/amd/Backbone', 'lib/has'], 
  (Backbone) ->

    class LocalStorageStoringStrategy
      
      store : (key, obj) ->
        localStorage.setItem(key, JSON.stringify(obj))

      fetch : (key, defaults) ->
        stored = localStorage.getItem(key)
        if stored != null then JSON.parse(stored) else defaults
        
      remove : (key) -> localStorage.removeItem(key)

    class InMemoryStoringStrategy
      
      constructor : () ->
        @storage = {}

      store : (key, obj) ->
        @storage[key] = obj

      fetch : (key, defaults) ->
        if @storage[key]? then @storage[key] else @defaults
        
      remove : (key) ->
        delete @storage[key]
    
    class LocalModelStore

      # Set to create a namespace
      storagePrefix : ''

      constructor : () ->
        _(this).extend(Backbone.Events)
        
        if has("native-localstorage")
          @storingStrategy = new LocalStorageStoringStrategy()
        else
          @storingStrategy = new InMemoryStoringStrategy()
        @_cache = {}

      ### Fetch and unserialize an object of the given type,
      stored at the given storage key.
      
      Once unserialized, resulting objects #setSaveMethod
      and #setFetchMethod are called. The provided save method
      expects the uneserialized object as an argument, and
      the provided fetch method returns the raw saved JSON.
      
      toJSON is used to serialize objects, and the objects
      constructor is passed the raw JSON upon instantiation.
      ###
      get : (key,type=null,defaults=null) ->
        if not @_cache[key]?
          fetch = () =>
            @storingStrategy.fetch key, defaults
          save = (item) =>
            @set key, item
          
          if type?
            item = new type fetch()
            item.setSaveMethod save if item.setSaveMethod? 
            item.setFetchMethod fetch if item.setFetchMethod? 
          else
            item = fetch()
          
          @_cache[key] = item
          
        return @_cache[key]
        
      set : (key, item) ->
        @_cache[key] = item
        
        item = item.toJSON() if item != null and item.toJSON
        if @storagePrefix.length > 0
          key = "#{@storagePrefix}::#{key}"
        @storingStrategy.store(key, item)
        
        @trigger "change"
        @trigger "change:#{key}"
        
      remove : (key) ->
        @storingStrategy.remove(key)

)

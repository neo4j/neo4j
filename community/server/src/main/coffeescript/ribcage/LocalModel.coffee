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

    class LocalModel extends Backbone.Model
      
      constructor : (args, opts) ->
        @_nestedModels = []
        super(args, opts)
      
      get : (key, defaultValue=null) ->
        val = super key
        if not val
          return defaultValue
        val
        
      set : (update, val, opts) ->
        if _(update).isString()
          updateMap = {}
          updateMap[update] = val
          update = updateMap
        else
          opts = val
        super update, opts
      
      fetch : (prop = null) ->
        json = @_fetch()
        if prop == null
          @clear silent:true
          @set json
          
          
        else 
          return json[prop]
        
      save : () ->
        @_save this
        
      ### Boilerplate to set 
      a model or collection as an attribute.
      
      Takes care of setting fetch and save
      methods on the model/collection appropriately,
      and adds hooks into this models fetch
      and toJSON methods to ensure fetch calls
      and saves propagate correctly.
      
      @param name is the property name to use and to fetch data via
      @param type is the model or collection class (or any object with a deserialize method)
      ###
      initNestedModel : (name, type) ->
        if type.deserialize?
          @[name] = type.deserialize @get name
        else
          @[name] = new type @get name
        @[name].setFetchMethod () =>
          @fetch(name)
        @[name].setSaveMethod @save
        
        @_nestedModels.push name
        
      toJSON : () ->
        data = super()
        data.id = @id
        
        for name in @_nestedModels
          data[name] = @[name].toJSON()
        
        return data
        
      setFetchMethod : (@_fetch) ->
      setSaveMethod : (@_save) ->
)

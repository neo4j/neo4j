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
  ['neo4j/webadmin/utils/ItemUrlResolver'
   './Property'
   'ribcage/Model'], 
  (ItemUrlResolver, Property, Model) ->
  
    ID_COUNTER = 0

    class PropertyContainer extends Model
      
      defaults :
        status : "saved"

      # Item should be the node or relationship we will wrap,
      # reportError is a callback for reporting errors
      # we don't know how to recover from.
      constructor : (item, @reportError, opts={}) ->
        super(opts)
        @properties = {}

        @urlResolver = new ItemUrlResolver

        @item = item
        @properties = {}
        for key, value of @getItem().getProperties()
          @addProperty(key, value, {silent:true})

        @setSaved()
        @updatePropertyList()

      getItem : () =>
        @item
        
      getSelf : () =>
        @getItem().getSelf()

      getId : () =>
        if @item instanceof neo4j.models.Node
          @urlResolver.extractNodeId(@getSelf())
        else
          @urlResolver.extractRelationshipId(@getSelf())

      setKey : (id, key, opts={}) =>
        duplicate = @hasKey(key, id)
        property = @getProperty(id)

        oldKey = property.getKey()
        property.set "key": key
        
        @setNotSaved()

        @getItem().removeProperty(oldKey)

        if duplicate
          property.setKeyError "This key is already used, please choose a different one."
        else
          property.setKeyError false
          @getItem().setProperty(key, property.getValue())

        @updatePropertyList(opts)
   
      setValue : (id, value, opts={}) =>
        property = @getProperty(id)
        cleanedValue = @cleanPropertyValue(value)
        
        @setNotSaved()

        if cleanedValue.value?
          property.set "valueError": false
          property.set "value": cleanedValue.value

          @getItem().setProperty(property.getKey(), cleanedValue.value)

        else
          property.set "value": value
          property.set "valueError": cleanedValue.error
        @updatePropertyList(opts)
        return property

      deleteProperty : (id, opts={}) =>
        @setNotSaved()

        property = @getProperty(id)
        delete(@properties[id])

        @getItem().removeProperty property.getKey()

        potentialDuplicate = @getPropertyByKey(property.getKey())
        if potentialDuplicate
          @setKey(potentialDuplicate.getLocalId(), potentialDuplicate.getKey(), opts)

        @updatePropertyList(opts)
        @trigger("remove:property")

      addProperty : (key="", value="", opts={}) =>

        id = @generatePropertyId()
        @properties[id] = new Property({key:key, value:value, localId:id})
        @updatePropertyList(opts)
        @trigger("add:property")

      getProperty : (id) =>
        @properties[id]

      getPropertyByKey : (key, ignoreId=null) =>
        for id, property of @properties
          if property.getKey() is key and parseInt(id) isnt parseInt(ignoreId)
            return property

        return null

      hasKey : (search, ignoreId=null) =>
        @getPropertyByKey(search, ignoreId) != null

      updatePropertyList : (opts={}) =>
        flatProperties = []
        for key, property of @properties
          flatProperties.push(property)
        
        silent = opts.silent? and opts.silent is true
        opts.silent = true
        @set { propertyList : flatProperties }, opts

        if not silent
          @trigger("change:propertyList")

      save : () =>
        if @noErrors()
          @setSaveState("saving")
          @getItem().save().then @setSaved, @saveFailed

      saveFailed : (response) =>
        @setNotSaved()
        @reportError(response.error || response)

      setSaved : () =>
        @setSaveState("saved")

      setNotSaved : () =>
        @setSaveState("notSaved")

      isSaved : =>
        @getSaveState() == "saved"

      isNotSaved : => 
        @getSaveState() == "notSaved"

      getSaveState : =>
        @get "status"
      
      setSaveState : (state, opts={}) =>
        @set { status : state }

      noErrors : (opts={}) =>
        for id, property of @properties
          if not (opts.ignore?) or opts.ignore != id
            if property.hasKeyError() or property.hasValueError()
              return false

        return true
      
      cleanPropertyValue : (rawVal) =>
        try
          val = JSON.parse rawVal
          if val == null
            return error:"Null values are not allowed. Please use strings, numbers or arrays."
          else if @isMap val
            return error:"Maps are not supported property values. Please use strings, numbers or arrays."
          else if _(val).isArray() and not @isValidArrayValue val
            return error:"Only arrays with one type of values, and only primitive types, is allowed."
          else
            return value:val
        catch e
          return error:"This does not appear to be a valid JSON value. Valid values are JSON strings, numbers or arrays. For instance 1.2, \"bob\" and [1,2,3]."


      isMap : (val) => 
        return JSON.stringify(val).indexOf("{") == 0

      isValidArrayValue : (val) =>
        if val.length == 0
          return true

        firstValue = val[0]
        if _.isString firstValue
          validType = _.isString 
        else if _.isNumber firstValue
          validType = _.isNumber
        else if _.isBoolean firstValue
          validType = _.isBoolean
        else 
          return false

        for value in val
          if not validType value
            return false;

        return true;


      generatePropertyId : () =>
        ID_COUNTER++
)

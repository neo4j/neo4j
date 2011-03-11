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

define ['./Property','lib/backbone'], (Property) ->
  
  ID_COUNTER = 0

  class PropertyContainer extends Backbone.Model
    
    defaults :
      saveState : "saved"

    initialize : (opts) =>
      @properties = {}

    setDataModel : (dataModel) =>
      @dataModel = dataModel
      @properties = {}
      for key, value of @getItem().getProperties()
        @addProperty(key, value, {silent:true})

      @setSaved()
      @updatePropertyList()

    getItem : () =>
       @dataModel.get("data")
      
    getSelf : () =>
      @dataModel.get("data").getSelf()

    setKey : (id, key, opts={}) =>
      duplicate = @hasKey(key, id)
      property = @getProperty(id)

      oldKey = property.key
      property.set "key": key
      
      if @validate()
        @setNotSaved()

      if duplicate
        property.setKeyError "This key is already used, please choose a different one."
      else
        property.setKeyError false

        @getItem().removeProperty(oldKey)
        @getItem().setProperty(key, property.getValue())

      @updatePropertyList(opts)
 
    setValue : (id, value, opts={}) =>
      property = @getProperty(id)
      cleanedValue = @cleanPropertyValue(value)
      if not @isCantSave()
        @setNotSaved()

      if cleanedValue.value?
        property.set "valueError": false
        property.set "value": cleanedValue.value

        @getItem().setProperty(property.getKey(), cleanedValue.value)

      else
        property.set "value": null
        property.set "valueError": cleanedValue.error
      @updatePropertyList(opts)

    deleteProperty : (id, opts={}) =>
      if not @isCantSave()
        @setNotSaved()

        property = @getProperty(id)
        delete(@properties[id])

        @getItem().removeProperty property.getKey()
        @updatePropertyList(opts)

    addProperty : (key="", value="", opts={}) =>

      id = @generatePropertyId()
      @properties[id] = new Property({key:key, value:value, localId:id})
      @updatePropertyList(opts)

    getProperty : (id) =>
      @properties[id]

    hasKey : (search, ignoreId=null) =>
      for id, property of @properties
        if property.getKey() == search and id != ignoreId
          return true

      return false

    updatePropertyList : (opts={silent:true}) =>
      flatProperties = []
      for key, property of @properties
        flatProperties.push(property)

      @set { propertyList : flatProperties) }, opts

    hasDuplicates : =>
      for key, property of @properties
        if property.isDuplicate()
          return true

      return false

    save : () =>
      @setSaveState("saving")
      @getItem().save().then @setSaved, @saveFailed

    saveFailed : (ev) =>
      @setNotSaved()

    setSaved : () =>
      @setSaveState("saved")

    setCantSave : () =>
      @setSaveState("cantSave")

    setNotSaved : () =>
      @setSaveState("notSaved")

    isSaved : =>
      @getSaveState() == "saved"

    isCantSave : () =>
      @getSaveState() == "cantSave"

    isNotSaved : => 
      @getSaveState() == "notSaved" or @isCantSave()

    getSaveState : =>
      @get "saveState"
    
    setSaveState : (state, opts={}) =>
      @set { saveState : state }, opts


    validate : =>
      
      for property in @properties
        if property.hasKeyError() or property.hasValueError()
          return false

    
    cleanPropertyValue : (rawVal) =>
      try
        val = JSON.parse rawVal
        if  val == null
          return error:"Null values are not allowed."
        else if @isMap val
          return error:"Maps are not supported property values."
        else if _(val).isArray() and not @isValidArrayValue val
          return error:"Only arrays with one type of values, and only primitive types, is allowed."
        else
          return value:val
      catch e
        return error:"This does not appear to be a valid JSON value."


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

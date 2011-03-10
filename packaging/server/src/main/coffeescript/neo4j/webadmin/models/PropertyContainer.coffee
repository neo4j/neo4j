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
        @addProperty(key, value, false, {saved:true},{silent:true})

      @setSaved()
      @updatePropertyList()

    getItem : () =>
       @dataModel.get("data")
      
    getSelf : () =>
      @dataModel.get("data").getSelf()

    setKey : (id, key) =>
      duplicate = @hasKey(key, id)
      property = @getProperty(id)

      oldKey = property.key
      property.set "key": key
      
      if not @isCantSave()
        @setNotSaved()

      if duplicate
        property.set "isDuplicate": true
        @setCantSave()
      else
        property.set "isDuplicate": false

        if not @hasDuplicates()
          @setNotSaved()

        @getItem().removeProperty(oldKey)
        @getItem().setProperty(key, property.getValue())
      @updatePropertyList()
 
    setValue : (id, value) =>
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
      @updatePropertyList()

    deleteProperty : (id, updatePropertyList=true, opts={}) =>
      if not @isCantSave()
        @setNotSaved()

        property = @getProperty(id)
        delete(@properties[id])

        @getItem().removeProperty property.getKey()
        if updatePropertyList
          @updatePropertyList(opts)

    addProperty : (key="", value="", updatePropertyList=true, propertyMeta={}, opts={}) =>
      id = @generatePropertyId()

      isDuplicate = if propertyMeta.isDuplicate? then true else false

      @properties[id] = new Property({key:key, value:value, localId:id, isDuplicate:isDuplicate})
       
      if updatePropertyList
        @updatePropertyList(opts)

    getProperty : (id) =>
      @properties[id]

    hasKey : (search, ignoreId=null) =>
      for id, property of @properties
        if property.getKey() == search and id != ignoreId
          return true

      return false

    updatePropertyList : (opts={silent:true}) =>
      @set { propertyList : @getPropertyList() }, opts
      @change()

    getPropertyList : () =>
      arrayed = []
      for key, property of @properties
        arrayed.push(property)

      return arrayed

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

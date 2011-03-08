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

define ['lib/backbone'], () ->
  
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
      property.key = key
      
      if not @isCantSave()
        @setNotSaved()

      property.saved = false

      if duplicate
        property.isDuplicate = true
        @setCantSave()
      else
        property.isDuplicate = false

        if not @hasDuplicates()
          @setNotSaved()

        @getItem().removeProperty(oldKey)
        @getItem().setProperty(key, property.value)
      @updatePropertyList()
 
    setValue : (id, value) =>
      if not @isCantSave()
        @setNotSaved()

    deleteProperty : (id) =>
      if not @isCantSave()
        @setNotSaved()
        property = @getProperty(id)
        @getItem().removeProperty(oldKey)

    addProperty : (key="", value="", updatePropertyList=true, propertyMeta={}, opts={}) =>
      id = @generatePropertyId()

      isDuplicate = if propertyMeta.isDuplicate? then true else false
      saved = if propertyMeta.saved? then true else false

      @properties[id] = {key:key, value:value, id:id, isDuplicate:isDuplicate, saved:saved}
      if updatePropertyList
        @updatePropertyList(opts)

    getProperty : (id) =>
      @properties[id]

    hasKey : (search, ignoreId=null) =>
      for id, property of @properties
        if property.key == search and id != ignoreId
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
        if property.isDuplicate
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


    generatePropertyId : () =>
      ID_COUNTER++

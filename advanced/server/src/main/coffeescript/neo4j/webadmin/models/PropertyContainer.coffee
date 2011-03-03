
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
        @addProperty(key, value, false, false)

      @set {saveState : "saved"}, {silent:true}
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
      
      if duplicate
        property.isDuplicate = true
      else
        property.isDuplicate = false
        @getItem().removeProperty(oldKey)
        @getItem().setProperty(key, property.value)
        @save()
      @updatePropertyList()
 
    setValue : (id, value) =>
      console.log id, value

    deleteProperty : (id) =>
      console.log id

    addProperty : (key="", value="", isDuplicate=false, updatePropertyList=true) =>
      id = @generatePropertyId()
      @properties[id] = {key:key, value:value, id:id, isDuplicate:isDuplicate}
      if updatePropertyList
        @updatePropertyList()

    getProperty : (id) =>
      @properties[id]

    hasKey : (search, ignoreId=null) =>
      for id, property of @properties
        if property.key == search and id != ignoreId
          return true

      return false

    updatePropertyList : () =>
      @set { propertyList : @getPropertyList() }, {silent:true}
      @change()

    getPropertyList : () =>
      arrayed = []
      for key, property of @properties
        arrayed.push(property)

      return arrayed

    save : () =>
      @set { saveState : "saving" }
      @getItem().save().then @saveSucceeded, @saveFailed

    saveSucceeded : () =>
      @set { saveState : "saved" }

    saveFailed : (ev) =>
      @set { saveState : "saveFailed" }

    generatePropertyId : () =>
      ID_COUNTER++

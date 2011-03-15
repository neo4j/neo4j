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
  ['neo4j/webadmin/models/PropertyContainer',
   'neo4j/webadmin/templates/databrowser/propertyEditor','lib/backbone'], 
  (PropertyContainer, propertyEditorTemplate) ->

    class PropertyContainerView extends Backbone.View

      events : 
        "focus input.property-key"     : "focusedOnKeyField",
        "focus input.property-value"   : "focusedOnValueField",
        "keyup input.property-key"     : "keyChanged",
        "keyup input.property-value"   : "valueChanged",
        "change input.property-key"    : "keyChangeDone",
        "change input.property-value"  : "valueChangeDone",
        "click button.delete-property" : "deleteProperty",
        "click button.add-property"    : "addProperty",
        "click button.data-save-properties" : "saveChanges"

      initialize : (opts) =>
        @template = opts.template

      keyChanged : (ev) =>
        @propertyContainer.setNotSaved()

      valueChanged : (ev) =>
        @propertyContainer.setNotSaved()

      keyChangeDone : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setKey(id, $(ev.target).val())
        @saveChanges()

      valueChangeDone : (ev) =>    
        id = @getPropertyIdForElement(ev.target)
        el = $(ev.target)

        if @shouldBeConvertedToString(el.val())
          el.val('"' + el.val() + '"');
        
        @propertyContainer.setValue(id, el.val())
        @saveChanges()

      deleteProperty : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.deleteProperty(id)
        @propertyContainer.save()

      addProperty : (ev) =>
        @propertyContainer.addProperty()

      saveChanges : (ev) =>
        @propertyContainer.save()

      focusedOnKeyField : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @focusedField = { id:id, type:"key" }
      
      focusedOnValueField : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @focusedField = { id:id, type:"value" }
      
      updateSaveState : (ev) =>
        state = @propertyContainer.getSaveState()
        switch state
          when "saved" then @setSaveState "Saved", true
          when "notSaved" then @setSaveState "Not saved", false
          when "saving" then @setSaveState "Saving..", true
          when "cantSave" then @setSaveState "Can't save", true

      setSaveState : (text, disabled) =>
        button = $("button.data-save-properties", @el)
        button.html(text)
        if disabled
          button.attr("disabled","disabled")
        else
          button.removeAttr("disabled")
        
      getPropertyIdForElement : (element) =>
        $(element).closest("ul").find("input.property-id").val()

      setDataModel : (dataModel) =>
        @unbind()
        @propertyContainer = dataModel.getData()
        @propertyContainer.bind "change:propertyList", @renderProperties
        @propertyContainer.bind "change:status", @updateSaveState

      render : =>
        $(@el).html(@template(
          item : @propertyContainer
        ))
        @renderProperties()
        return this

      remove : =>
        @unbind()
        super()

      unbind : =>
        if @propertyContainer?
          @propertyContainer.unbind "change:propertyList", @renderProperties
          @propertyContainer.unbind "change:status", @updateSaveState


      renderProperties : =>
        $(".properties",@el).html(propertyEditorTemplate(
          properties : @propertyContainer.get "propertyList"
        ))

        if @focusedField?
          @getPropertyField(@focusedField.id, @focusedField.type)

        return this

      shouldBeConvertedToString : (val) =>
        try 
          JSON.parse val
          return false
        catch e
          return /^[a-z0-9-_\/\\\(\)#%\&!$]+$/i.test(val)

      getPropertyField : (id, type) =>
        
)

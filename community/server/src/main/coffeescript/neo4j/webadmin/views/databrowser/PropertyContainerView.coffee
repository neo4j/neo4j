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
        @propertyContainer = new PropertyContainer()
        @propertyContainer.bind "change:propertyList", @renderProperties
        @propertyContainer.bind "change:saveState", @updateSaveState

      keyChanged : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setKey(id, $(ev.target).val())

      valueChanged : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setValue(id, $(ev.target).val())

      keyChangeDone : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setKey(id, $(ev.target).val())
        @propertyContainer.save()

      valueChangeDone : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setValue(id, $(ev.target).val())
        @propertyContainer.save()

      deleteProperty : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.deleteProperty(id)

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
        $(element).closest("li").find("input.property-id").val()

      setDataModel : (dataModel) =>
        @dataModel = dataModel
        @propertyContainer.setDataModel(@dataModel)

      render : =>
        $(@el).html(@template(
          item : @propertyContainer
        ))
        @renderProperties()
        return this

      renderProperties : =>
        $(".properties",@el).html(propertyEditorTemplate(
          properties : @propertyContainer.get "propertyList"
        ))

        if @focusedField?
          @getPropertyField(@focusedField.id, @focusedField.type)

        return this

      getPropertyField : (id, type) =>
        
)

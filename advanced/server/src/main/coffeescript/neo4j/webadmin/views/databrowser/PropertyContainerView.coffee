define(
  ['neo4j/webadmin/models/PropertyContainer',
   'neo4j/webadmin/templates/databrowser/propertyEditor','lib/backbone'], 
  (PropertyContainer, propertyEditorTemplate) ->

    class PropertyContainerView extends Backbone.View

      events : 
        "focus input.property-key"     : "focusedOnKeyField",
        "focus input.property-value"   : "focusedOnValueField",
        "change input.property-key"     : "keyChanged",
        "change input.property-value"   : "valueChanged",
        "click  button.delete-property" : "deleteProperty",
        "click  button.add-property"    : "addProperty"

      initialize : (opts) =>
        @template = opts.template
        @propertyContainer = new PropertyContainer()
        @propertyContainer.bind "change", @renderProperties

      keyChanged : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setKey(id, $(ev.target).val())

      valueChanged : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.setValue(id, $(ev.target).val())

      deleteProperty : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @propertyContainer.deleteProperty(id)

      addProperty : (ev) =>
        @propertyContainer.addProperty()

      focusedOnKeyField : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @focusedField = { id:id, type:"key" }
      
      focusedOnValueField : (ev) =>
        id = @getPropertyIdForElement(ev.target)
        @focusedField = { id:id, type:"value" }

        
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

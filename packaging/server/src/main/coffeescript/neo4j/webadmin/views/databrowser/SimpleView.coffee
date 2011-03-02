define(
  ['neo4j/webadmin/templates/data/node',
   'neo4j/webadmin/templates/data/relationship',
   'neo4j/webadmin/templates/data/set',
   'neo4j/webadmin/templates/data/notfound','lib/backbone'], 
  (nodeTemplate, relationshipTemplate, setTemplate, notFoundTemplate) ->
  
    class SimpleView extends Backbone.View

      initialize : (options)->
        @dataModel = options.dataModel
        @dataModel.bind("change:item", @itemChanged)

      render : =>
        type = @dataModel.get("item").get("type")
        switch type
          when "node"
            template = nodeTemplate
          when "relationship"
            template = relationshipTemplate
          when "set"
            template = setTemplate
          else
            template = notFoundTemplate

        $(@el).html(template({ item : @dataModel.get("item").get("item") }))
        return this

      itemChanged : (ev) =>
        @render()
)

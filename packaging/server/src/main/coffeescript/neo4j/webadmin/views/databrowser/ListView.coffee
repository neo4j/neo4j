define(
  ['neo4j/webadmin/templates/databrowser/list',
   'lib/backbone'], 
  (template, PropertyEditorView) ->
  
    class ListView extends Backbone.View

      render : =>
        $(@el).html(template())
        return this
)

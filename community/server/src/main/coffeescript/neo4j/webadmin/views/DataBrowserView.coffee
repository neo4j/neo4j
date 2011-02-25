define ['neo4j/webadmin/templates/databrowser','lib/backbone'], (template) ->
  
  class DataBrowserView extends Backbone.View
    
    template : template

    render : ->
      $(@el).html(@template())
      return this

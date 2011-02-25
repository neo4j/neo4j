define ['neo4j/webadmin/templates/console','lib/backbone'], (template) ->
  
  class ConsoleView extends Backbone.View
    
    template : template

    render : ->
      $(@el).html(@template())
      return this

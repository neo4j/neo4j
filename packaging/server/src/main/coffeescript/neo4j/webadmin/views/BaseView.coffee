define ['neo4j/webadmin/templates/base','lib/backbone'], (template) ->
  
  class BaseView extends Backbone.View
    
    template : template
    
    render : ->
      $(@el).html(@template())
      return this
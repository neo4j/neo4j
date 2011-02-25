define ['neo4j/webadmin/templates/dashboard','lib/backbone'], (template) ->
  
  class DashboardView extends Backbone.View
    
    template : template

    render : ->
      $(@el).html(@template())
      return this

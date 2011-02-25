define ['neo4j/webadmin/templates/server_info','lib/backbone'], (template) ->
  
  class ServerInfoView extends Backbone.View
    
    template : template

    render : ->
      $(@el).html(@template())
      return this

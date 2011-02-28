define ['neo4j/webadmin/templates/base','lib/backbone'], (template) ->
  
  class BaseView extends Backbone.View
    
    template : template
    
    initialize : (options) =>
      @appState = options.appState
      @appState.bind 'change:mainView', (event) =>
        @mainView = event.attributes.mainView
        @render()

    render : ->
      $(@el).html @template( mainmenu : [ 
        { label : "Dashboard",   subtitle:"Get a grip",url : "#",           current: location.hash is "" }
        { label : "Data browser",subtitle:"Explore and edit",url : "#/data/" ,    current: location.hash is "#/data/" }
        { label : "Console",     subtitle:"Power tool",url : "#/console/" , current: location.hash is "#/console/" }
        { label : "Server info", subtitle:"Detailed information",url : "#/info/" ,    current: location.hash is "#/info/" } ] )

      if @mainView?
        $("#contents").append @mainView.render().el
      return this

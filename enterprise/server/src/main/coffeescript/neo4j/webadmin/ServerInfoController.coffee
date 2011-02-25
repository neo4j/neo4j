
define ['./views/ServerInfoView', 'lib/backbone'], (ServerInfoView) ->
  
  class ServerInfoController extends Backbone.Controller
    routes : 
      "/info/" : "base"

    initialize : (appState) =>
      @appState = appState

    base : =>
      @appState.set( mainView : @getServerInfoView() )

    getServerInfoView : =>
      @serverInfoView ?= new ServerInfoView(@appState)


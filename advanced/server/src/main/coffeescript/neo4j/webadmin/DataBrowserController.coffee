
define ['./views/DataBrowserView', 'lib/backbone'], (DataBrowserView) ->
  
  class DataBrowserController extends Backbone.Controller
    routes : 
      "/data/" : "base"

    initialize : (appState) =>
      @appState = appState

    base : =>
      @appState.set( mainView : @getDataBrowserView() )

    getDataBrowserView : =>
      @dataBrowserView ?= new DataBrowserView(@appState)


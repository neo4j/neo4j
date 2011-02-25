
define ['./views/DashboardView', 'lib/backbone'], (DashboardView) ->
  
  class ApplicationController extends Backbone.Controller
    routes : 
      "" : "dashboard"

    initialize : (appState) =>
      @appState = appState

    dashboard : =>
      @appState.set( mainView : @getDashboardView() )

    getDashboardView : =>
      @dashboardView ?= new DashboardView(@appState)


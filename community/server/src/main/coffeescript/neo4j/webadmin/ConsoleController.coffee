
define ['./views/ConsoleView', 'lib/backbone'], (ConsoleView) ->
  
  class ConsoleController extends Backbone.Controller
    routes : 
      "/console/" : "console"

    initialize : (appState) =>
      @appState = appState

    console : =>
      @appState.set( mainView : @getConsoleView() )

    getConsoleView : =>
      @consoleView ?= new ConsoleView(@appState)


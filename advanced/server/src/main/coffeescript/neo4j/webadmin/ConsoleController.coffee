
define ['./models/Console', './views/ConsoleView', 'lib/backbone'], (Console, ConsoleView) ->
  
  class ConsoleController extends Backbone.Controller
    routes : 
      "/console/" : "console"

    initialize : (appState) =>
      @appState = appState
      @consoleState = new Console(server:@appState.get("server"))

    console : =>
      @appState.set( mainView : @getConsoleView() )

    getConsoleView : =>
      @consoleView ?= new ConsoleView(
        appState : @appState
        consoleState : @consoleState)


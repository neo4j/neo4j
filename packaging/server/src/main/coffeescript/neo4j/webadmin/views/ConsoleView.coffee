define(
  ['neo4j/webadmin/templates/console/base',
   'neo4j/webadmin/templates/console/console',
   'lib/backbone'], 
  (baseTemplate, consoleTemplate) ->

    class ConsoleView extends Backbone.View
      
      events : 
        "keyup #console-input" : "consoleKeyUp"
        "click #console-base" : "wrapperClicked"

      initialize : (opts) =>
        @appState = opts.appState
        @consoleState = opts.consoleState
        @consoleState.bind("change", @renderConsole)

      consoleKeyUp : (ev) =>
        if ev.keyCode is 13 # ENTER
          @consoleState.eval $("#console-input").val()
        else if ev.keyCode is 38 # UP
          @consoleState.prevHistory()
        else if ev.keyCode is 40 # DOWN
          @consoleState.nextHistory()

      wrapperClicked : (ev) =>
        $("#console-input").focus()

      render : =>
        $(@el).html(baseTemplate())
        @renderConsole()
        return this

      renderConsole : =>
        $("#console",@el).html consoleTemplate(
          lines : @consoleState.get "lines"
          prompt : @consoleState.get "prompt"
          showPrompt : @consoleState.get "showPrompt")
        
        @delegateEvents()
        $("#console-input").focus()
        @scrollToBottomOfConsole()

      scrollToBottomOfConsole : () =>
        wrap = $("#console",@el)
        if wrap[0]
          wrap[0].scrollTop = wrap[0].scrollHeight

)

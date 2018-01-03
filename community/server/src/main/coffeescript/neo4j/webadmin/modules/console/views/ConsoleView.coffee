###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['./gremlin'
   './console'
   'ribcage/View'
   'lib/amd/jQuery'
   'lib/amd/jQuery.putCursorAtEnd'], 
  (baseTemplate, consoleTemplate, View, $ ) ->

    class ConsoleView extends View
      
      events : 
        "keyup #console-input" : "consoleKeyUp"
        "click #console-base" : "wrapperClicked"

      initialize : (opts) =>
        @appState = opts.appState
        @consoleState = opts.consoleState
        @lang = opts.lang
        @availableEngines = opts.engines
        @consoleState.bind("change", @renderConsole)

      consoleKeyUp : (ev) =>
        statement = $("#console-input").val()
        @consoleState.setStatement statement, silent : true
        
        if statement.length > 0
          $(".console-multiline-help",@el).hide()
        else
          $(".console-multiline-help",@el).show()
        
        
        if ev.keyCode is 13 # ENTER
          @consoleState.eval()
        else if ev.keyCode is 38 # UP
          @consoleState.prevHistory()
        else if ev.keyCode is 40 # DOWN
          @consoleState.nextHistory()

      wrapperClicked : (ev) =>
        @focusOnInputField()
        
      focusOnInputField :->
        $("#console-input").focus()
        $("#console-input").putCursorAtEnd()

      renderConsole : ()=>
        $("#console-base",@el).html consoleTemplate(          
          engines : @availableEngines
          lines : @consoleState.get "lines"
          prompt : @consoleState.get "prompt"
          showPrompt : @consoleState.get "showPrompt"
          showMultilineHelp : @consoleState.get "showMultilineHelp" or false
          current : @lang
          promptPrefix : @consoleState.get "promptPrefix")
        
        @delegateEvents()
        @scrollToBottomOfConsole()
        @focusOnInputField()
        
      scrollToBottomOfConsole : () =>
        wrap = $("#console",@el)
        if wrap[0]
          wrap[0].scrollTop = wrap[0].scrollHeight

      remove : =>
        @consoleState.unbind("change", @renderConsole)
        super()
)

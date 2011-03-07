###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

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

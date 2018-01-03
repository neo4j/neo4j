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
  ['./models/Console'
   './models/HttpConsole'
   './views/ShellConsoleView'
   './views/GremlinConsoleView'
   './views/HttpConsoleView'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'ribcage/Router'], 
  (Console, HttpConsole, ShellConsoleView, GremlinConsoleView, HttpConsoleView, MainMenuModel, Router) ->
  
    class ConsoleRouter extends Router
      routes : 
        "/console/"      : "showConsole"
        "/console/:type" : "showConsole"

      consoleType : "http"

      init : (appState) =>
        @appState = appState

        @menuItem = new MainMenuModel.Item 
          title : "Console",
          subtitle:"Power tool",
          url : "#/console/"

        @gremlinState = new Console(server:@appState.get("server"), lang:"gremlin")
        @shellState = new Console(server:@appState.get("server"), lang:"shell")
        @httpState = new HttpConsole(server:@appState.get("server"), lang:"http")
      
        # Ask the server what console engines are available
        self = this
        @appState.getServer().manage.console.availableEngines (engines) ->
          self.onAvailableEnginesLoaded(engines)
          
      showConsole : (type=false) =>
        @saveLocation()

        if type is false then type = @consoleType
        @consoleType = type

        if @views?
          if @views[type]?
            view = @views[type]            
          else
            alert "Unsupported console type: '#{type}', is it disabled in the server?."
            view = @views['http']
          
          @appState.set( mainView : view )
          view.focusOnInputField()
        else
          # Set a flag to remember to re-run this method when
          # available engines has been loaded.
          @renderWhenEnginesAreLoaded = true

      onAvailableEnginesLoaded : (engines) ->
        engines.push('http') # HTTP is always available
        @views = 
          http : new HttpConsoleView
            appState     : @appState
            consoleState : @httpState
            lang         : "http"
            engines      : engines

        if _(engines).indexOf('gremlin') > -1
          @views.gremlin = new GremlinConsoleView
            appState     : @appState
            consoleState : @gremlinState
            lang         : "gremlin"
            engines      : engines

        if  _(engines).indexOf('shell') > -1 
          @views.shell = new ShellConsoleView
            appState     : @appState
            consoleState : @shellState
            lang         : "shell"
            engines      : engines
          
          # Use shell per default if it is available
          @consoleType = "shell"

        if @renderWhenEnginesAreLoaded?
          @showConsole()

      #
      # Bootstrapper SPI
      #

      getMenuItems : ->
        [@menuItem]
)

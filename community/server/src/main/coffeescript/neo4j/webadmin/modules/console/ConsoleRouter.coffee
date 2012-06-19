###
Copyright (c) 2002-2012 "Neo Technology,"
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
   'ribcage/Router'], 
  (Console, HttpConsole, ShellConsoleView, GremlinConsoleView, HttpConsoleView, Router) ->
  
    class ConsoleRouter extends Router
      routes : 
        "/console/" : "console"
        "/console/:type" : "console"

      consoleType : "shell"

      init : (appState) =>
        @appState = appState
        @gremlinState = new Console(server:@appState.get("server"), lang:"gremlin")
        @shellState = new Console(server:@appState.get("server"), lang:"shell")
        @httpState = new HttpConsole(server:@appState.get("server"), lang:"http")
      
        @views = 
          gremlin : new GremlinConsoleView
            appState : @appState
            consoleState : @gremlinState
            lang: "gremlin"
          shell : new ShellConsoleView
            appState : @appState
            consoleState : @shellState
            lang: "shell"
          http : new HttpConsoleView
            appState : @appState
            consoleState : @httpState
            lang: "http"
          
      console : (type=false) =>
        @saveLocation()
        if type is false then type = @consoleType
        @consoleType = type
        @appState.set( mainView : @getConsoleView(type) )
        @getConsoleView(type).focusOnInputField()

      getConsoleView : (type) =>
        @views[type]
)

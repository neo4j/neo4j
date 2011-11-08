###
Copyright (c) 2002-2011 "Neo Technology,"
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
   './models/CypherConsole'
   './models/HttpConsole'
   './views/CypherConsoleView'
   './views/GremlinConsoleView'
   './views/HttpConsoleView'
   'ribcage/Router'
   'lib/backbone'], 
  (Console, CypherConsole, HttpConsole, CypherConsoleView, GremlinConsoleView, HttpConsoleView, Router) ->
  
    class ConsoleRouter extends Router
      routes : 
        "/console/" : "console"
        "/console/:type" : "console"

      consoleType : "cypher"

      init : (appState) =>
        @appState = appState
        @gremlinState = new Console(server:@appState.get("server"), lang:"gremlin")
        @cypherState = new CypherConsole(server:@appState.get("server"), lang:"cypher")
        @httpState = new HttpConsole(server:@appState.get("server"), lang:"http")
      
        @views = 
          gremlin : new GremlinConsoleView
            appState : @appState
            consoleState : @gremlinState
            lang: "gremlin"
          cypher : new CypherConsoleView
            appState : @appState
            consoleState : @cypherState
            lang: "cypher"
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

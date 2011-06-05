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
  ['./models/Console',
   './models/TBDConsole',
   './views/TBDConsoleView',
   './views/GremlinConsoleView',
   'lib/backbone'], 
  (Console, TBDConsole, TBDConsoleView, GremlinConsoleView) ->
  
    class ConsoleController extends Backbone.Controller
      routes : 
        "/console/" : "console"
        "/console/:type" : "console"

      initialize : (appState) =>
        @appState = appState
        @gremlinState = new Console(server:@appState.get("server"), lang:"gremlin")
        @tbdState = new TBDConsole(server:@appState.get("server"), lang:"tbd")
      
        @views = 
          gremlin : new GremlinConsoleView
            appState : @appState
            consoleState : @gremlinState
            lang: "gremlin"
          tbd : new TBDConsoleView
            appState : @appState
            consoleState : @tbdState
            lang: ""
          
      console : (type="gremlin") =>
        @appState.set( mainView : @getConsoleView(type) )

      getConsoleView : (type) =>
        @views[type]
)
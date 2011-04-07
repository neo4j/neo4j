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

window.log = () ->
  if $("#messages").length is 0
    $("body").append("<ul id='messages'></ul>")
  
  for item in arguments
   $("#messages").append("<li>#{item}")


require(
  ["neo4j/webadmin/DashboardController"
   "neo4j/webadmin/DataBrowserController"
   "neo4j/webadmin/ConsoleController"
   "neo4j/webadmin/ServerInfoController"
   "neo4j/webadmin/models/ApplicationState"
   "neo4j/webadmin/views/BaseView"
   "neo4j/webadmin/ui/FoldoutWatcher"
   "neo4j/webadmin/KeyboardShortcuts"
   "neo4j/webadmin/templates/splash"
   "lib/jquery"
   "lib/neo4js"
   "lib/backbone"]
  (DashboardController, DataBrowserController, ConsoleController, ServerInfoController, ApplicationState, BaseView, FoldoutWatcher, KeyboardShortcuts, splashTemplate) ->

    # WEBADMIN BOOT

    appState = new ApplicationState
    appState.set server : new neo4j.GraphDatabase(location.protocol + "//" + location.host)

    baseView = new BaseView(appState:appState)

    dashboardController   = new DashboardController appState
    databrowserController = new DataBrowserController appState
    consoleController     = new ConsoleController appState
    serverInfoController  = new ServerInfoController appState

    foldoutWatcher = new FoldoutWatcher
    foldoutWatcher.init()

    shortcuts = new KeyboardShortcuts(dashboardController, databrowserController, consoleController, serverInfoController)
    shortcuts.init()

    launchApp = ->
      $("body").empty().append(baseView.el)
      Backbone.history.start()

    # Show boot screen for flashiness
    #$("body").html(splashTemplate())

    launchApp()
    #setTimeout launchApp, 2000
)

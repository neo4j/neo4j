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
  ['./models/ServerInfo'
   './views/ServerInfoView'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'ribcage/Router'],
  (ServerInfo, ServerInfoView, MainMenuModel, Router) ->
    
    class ServerInfoRouter extends Router
      routes : 
        "/info/" : "base",
        "/info/:domain/:name/" : "bean",

      init : (@appState) =>
        
        @menuItem = new MainMenuModel.Item 
          title : "Server info",
          subtitle:"Details",
          url : "#/info/"

        @serverInfo = new ServerInfo { server : @appState.get "server" } 
        @server = @appState.get "server"

      base : =>
        @saveLocation()
        @appState.set( mainView : @getServerInfoView() )
        @serverInfo.fetch()

      bean : (domain, name) => 
        @saveLocation()
        @appState.set( mainView : @getServerInfoView() )
        @serverInfo.setCurrent(decodeURIComponent(domain), decodeURIComponent(name))

      getServerInfoView : =>
        @view ?= new ServerInfoView
          appState:@appState
          serverInfo:@serverInfo

      #
      # Bootstrapper SPI
      #

      getMenuItems : ->
        [@menuItem]
)

###
Copyright (c) 2002-2015 "Neo Technology,"
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
  ['./views/IndexManagerView'
   './models/IndexManager'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'ribcage/Router'],
  (IndexManagerView, IndexManager, MainMenuModel, Router) ->
  
    class IndexManagerRouter extends Router
      routes : 
        "/index/" : "idxManager"

      init : (appState) =>
        @appState = appState
        
        @menuItem = new MainMenuModel.Item 
          title : "Indexes",
          subtitle:"Add and remove",
          url : "#/index/"

        @idxMgr = new IndexManager(server:@appState.get("server"))

      idxManager : =>
        @saveLocation()
        @appState.set( mainView : @getIndexManagerView() )

      getIndexManagerView : =>
        @view ?= new IndexManagerView  
          state   : @appState
          idxMgr  : @idxMgr

      #
      # Bootstrapper SPI
      #

      getMenuItems : ->
        [@menuItem]
)

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
  ['./views/DashboardView'
   './models/ServerPrimitives'
   './models/DiskUsage'
   './models/CacheUsage'
   './models/ServerStatistics'
   './models/DashboardState'
   './models/KernelBean'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'ribcage/Router'],
  (DashboardView, ServerPrimitives, DiskUsage, CacheUsage, ServerStatistics, DashboardState, KernelBean, MainMenuModel, Router) ->
  
    class DashboardRouter extends Router
      routes : 
        "" : "dashboard"

      init : (appState) =>
        @appState = appState
        
        @menuItem = new MainMenuModel.Item 
          title : "Dashboard",
          subtitle:"Overview",
          url : "#"

      dashboard : =>
        @saveLocation()
        @appState.set( mainView : @getDashboardView() )

      getDashboardView : =>
        @view ?= new DashboardView  
          state      : @appState
          dashboardState : @getDashboardState()
          primitives : @getServerPrimitives()
          diskUsage  : @getDiskUsage()
          statistics : @getServerStatistics()
          kernelBean : @getKernelBean()

      getServerPrimitives : =>
        @serverPrimitives ?= new ServerPrimitives( server : @appState.getServer(), pollingInterval : 5000 )

      getKernelBean : =>
        @kernelBean ?= new KernelBean( server : @appState.getServer(), pollingInterval : 10000 )

      getDiskUsage : =>
        @diskUsage ?= new DiskUsage( server : @appState.getServer(), pollingInterval : 5000 )
      
      getServerStatistics : =>
        @serverStatistics ?= new ServerStatistics( server : @appState.getServer() )
      
      getDashboardState : =>
        @dashboardState ?= new DashboardState( server : @appState.getServer() )

      #
      # Bootstrapper SPI
      #

      getMenuItems : ->
        [@menuItem]

)

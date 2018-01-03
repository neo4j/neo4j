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

require(
  ["order!lib/jquery"
   "order!lib/neo4js"
   "order!lib/backbone"
   "neo4j/webadmin/modules/dashboard/DashboardRouter"
   "neo4j/webadmin/modules/databrowser/DataBrowserRouter"
   "neo4j/webadmin/modules/console/ConsoleRouter"
   "neo4j/webadmin/modules/serverinfo/ServerInfoRouter"
   "neo4j/webadmin/modules/indexmanager/IndexManagerRouter"
   "neo4j/webadmin/modules/baseui/BaseUI"
   "neo4j/webadmin/modules/moreinfo/MoreInfo"
   "neo4j/webadmin/modules/guide/Guide"
   "neo4j/webadmin/modules/loading/GlobalLoadingIndicator"
   "neo4j/webadmin/modules/connectionmonitor/ConnectionMonitor"
   "neo4j/webadmin/Bootstrapper"]
  (a,b,c,DashboardRouter, DataBrowserRouter, ConsoleRouter, ServerInfoRouter, IndexManagerRouter, BaseUI, MoreInfo, Guide, GlobalLoadingIndicator, ConnectionMonitor, Bootstrapper) ->

    modules = [
        new BaseUI
        new DashboardRouter
        new DataBrowserRouter
        new ConsoleRouter
        new IndexManagerRouter
        new ServerInfoRouter

        new ConnectionMonitor
        #new SplashScreen
        new GlobalLoadingIndicator
        new MoreInfo
    ]
    
    boot = new Bootstrapper
    boot.bootstrap modules

)

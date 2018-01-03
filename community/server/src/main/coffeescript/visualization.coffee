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
   "neo4j/webadmin/modules/databrowser/StandaloneVisualizationRouter"
   "neo4j/webadmin/modules/moreinfo/MoreInfo"
   "neo4j/webadmin/modules/loading/GlobalLoadingIndicator"
   "neo4j/webadmin/modules/connectionmonitor/ConnectionMonitor"
   "neo4j/webadmin/ApplicationState"
   "ribcage/security/HtmlEscaper"]
  (j,n,b,StandaloneVisualizationRouter, MoreInfo, GlobalLoadingIndicator, ConnectionMonitor, ApplicationState, HtmlEscaper) ->

    # Global html escaper, used by the pre-compiled templates.
    htmlEscaper = new HtmlEscaper()
    window.htmlEscape = htmlEscaper.escape

    # WEBADMIN BOOT

    appState = new ApplicationState
    appState.set server : new neo4j.GraphDatabase(location.protocol + "//" + location.host)

    modules = [
        new StandaloneVisualizationRouter
        new ConnectionMonitor
        new GlobalLoadingIndicator
        new MoreInfo
    ]

    jQuery () ->
      m.init(appState) for m in modules
      if Backbone.history
        Backbone.history.start()

)

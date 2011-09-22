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
  ["neo4j/webadmin/ApplicationState"
   "ribcage/security/HtmlEscaper"
   "lib/jquery"
   "lib/neo4js"
   "lib/backbone"], 
  (ApplicationState, HtmlEscaper) ->
  
    class Bootstrapper

      bootstrap : (modules) ->

        # Global html escaper, used by the pre-compiled templates.
        htmlEscaper = new HtmlEscaper()
        window.htmlEscape = htmlEscaper.escape

        # JQuery global setup
        jQuery.ajaxSetup({
        timeout : 1000 * 60 * 60 * 6 # Let requests run up to six hours
        })

        appState = new ApplicationState
        appState.set server : new neo4j.GraphDatabase(location.protocol + "//" + location.host)

        jQuery () ->
        m.init(appState) for m in modules
        Backbone.history.start()

)

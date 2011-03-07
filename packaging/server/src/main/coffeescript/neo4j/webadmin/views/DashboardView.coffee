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

define( 
  ['neo4j/webadmin/templates/dashboard',
   './DashboardInfoView',        
   'lib/backbone'], 
  (template, DashboardInfoView) ->

    class DashboardView extends Backbone.View
      
      template : template
     
      initialize : (opts) =>
        @appState = opts.state
        @infoView = new DashboardInfoView(opts)

      render : =>
        $(@el).html @template(
          server : { url : "someurl", version : "someversion" } )
        $("#dashboard-info", @el).append(@infoView.render().el)
        return this
)

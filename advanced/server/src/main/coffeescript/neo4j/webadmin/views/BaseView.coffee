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

define ['neo4j/webadmin/templates/base','lib/backbone'], (template) ->
  
  class BaseView extends Backbone.View
    
    template : template
    
    initialize : (options) =>
      @appState = options.appState
      @appState.bind 'change:mainView', (event) =>
        @mainView = event.attributes.mainView
        @render()

    render : ->
      $(@el).html @template( mainmenu : [ 
        { label : "Dashboard",   subtitle:"Get a grip",url : "#",           current: location.hash is "" }
        { label : "Data browser",subtitle:"Explore and edit",url : "#/data/" ,    current: location.hash is "#/data/" }
        { label : "Console",     subtitle:"Power tool",url : "#/console/" , current: location.hash is "#/console/" }
        { label : "Server info", subtitle:"Detailed information",url : "#/info/" ,    current: location.hash is "#/info/" } ] )

      if @mainView?
        $("#contents").append @mainView.render().el
      return this

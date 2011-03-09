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
  ['neo4j/webadmin/data/Search',
   './views/DataBrowserView', 
   './models/DataBrowserState', 
   './models/DataItem', 'lib/backbone'], 
  (Search, DataBrowserView, DataBrowserState, DataItem) ->
  
    DEFAULT_QUERY = "node:0"

    class DataBrowserController extends Backbone.Controller
      routes : 
        "/data/" : "base",
        "/data/search/:query/" : "search"

      initialize : (appState) =>
        @appState = appState
        @server = appState.get "server"
        @searcher = new Search(@server)
        @dataModel = new DataBrowserState( server : @server )

        @dataModel.bind "change:query", @queryChanged

      base : =>
        location.hash = "#/data/search/#{DEFAULT_QUERY}/"

      search : (query) =>
        @appState.set( mainView : @getDataBrowserView() )
        @dataModel.setQuery query

      queryChanged : =>
        url = "#/data/search/#{@dataModel.getEscapedQuery()}/"
        if location.hash != url
          location.hash = url

        if @dataModel.get "queryOutOfSyncWithData"
          @searcher.exec(@dataModel.get "query").then(@showResult, @showResult)

      showResult : (result) =>
        @dataModel.setData(result)

      getDataBrowserView : =>
        @dataBrowserView ?= new DataBrowserView({state:@appState, dataModel:@dataModel})
)

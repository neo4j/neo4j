###
  Copyright (c) 2002-2012 "Neo Technology,"
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
  ['./search/QueuedSearch',
   './views/DataBrowserView',
   './visualization/views/VisualizationSettingsView',
   './visualization/views/VisualizationProfileView',
   './models/DataBrowserState', 
   './DataBrowserSettings', 
   'ribcage/Router'], 
  (QueuedSearch, DataBrowserView, VisualizationSettingsView, VisualizationProfileView, DataBrowserState, DataBrowserSettings, Router) ->

    class DataBrowserRouter extends Router

      routes : 
        "/data/" : "base"
        "/data/search/*query" : "search"
        "/data/visualization/settings/" : "visualizationSettings"
        "/data/visualization/settings/profile/" : "createVisualizationProfile"
        "/data/visualization/settings/profile/:id/" : "editVisualizationProfile"

      shortcuts : 
        "s" : "focusOnSearchField"

      init : (appState) =>
        @appState = appState
        @server = appState.get "server"
        @searcher = new QueuedSearch(@server)

        @dataModel = new DataBrowserState( server : @server )

        @dataModel.bind "change:query", @queryChanged

      base : =>
        @queryChanged()

      search : (query) =>
        @saveLocation()
        query = decodeURIComponent query
        while query.charAt(query.length-1) == "/"
          query = query.substr(0, query.length - 1)

        @dataModel.setQuery query
        @appState.set( mainView : @getDataBrowserView() )

      visualizationSettings : () =>
        @saveLocation()
        @visualizationSettingsView ?= new VisualizationSettingsView
          dataBrowserSettings : @getDataBrowserSettings()
        @appState.set mainView : @visualizationSettingsView
        
      createVisualizationProfile : () =>
        @saveLocation()
        v = @getVisualizationProfileView()
        v.setIsCreateMode(true)
        @appState.set mainView : v
        
      editVisualizationProfile : (id) =>
        @saveLocation()
        profiles = @getDataBrowserSettings().getVisualizationProfiles()
        profile = profiles.get id
        
        v = @getVisualizationProfileView()
        v.setProfileToManage profile
        @appState.set mainView : v

      focusOnSearchField : (ev) =>
        @base()
        setTimeout( () -> 
          $("#data-console").focus()
        1)

      queryChanged : =>
        query = @dataModel.get "query"
        if query == null
          return @search("0")

        url = "#/data/search/#{encodeURIComponent(query)}/"

        if location.hash != url
          location.hash = url
        
        if @dataModel.get "queryOutOfSyncWithData"
          @searcher.exec(@dataModel.get "query").then(@showResult, @showResult)

      showResult : (result) =>
        @dataModel.setData(result)

      getDataBrowserView : =>
        @view ?= new DataBrowserView
          state:@appState
          dataModel:@dataModel

      getVisualizationProfileView : =>
        @visualizationProfileView ?= new VisualizationProfileView 
          dataBrowserSettings:@getDataBrowserSettings()
          
      getDataBrowserSettings : ->
        @dataBrowserSettings ?= new DataBrowserSettings @appState.getSettings()
)

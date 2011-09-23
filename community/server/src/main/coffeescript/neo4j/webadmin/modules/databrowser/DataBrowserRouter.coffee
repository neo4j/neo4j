
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
        query = decodeURIComponent query
        while query.charAt(query.length-1) == "/"
          query = query.substr(0, query.length - 1)

        @dataModel.setQuery query
        @appState.set( mainView : @getDataBrowserView() )

      visualizationSettings : () =>
        @visualizationSettingsView ?= new VisualizationSettingsView
          dataBrowserSettings : @getDataBrowserSettings()
        @appState.set mainView : @visualizationSettingsView
        
      createVisualizationProfile : () =>
        v = @getVisualizationProfileView()
        v.setIsCreateMode(true)
        @appState.set mainView : v
        
      editVisualizationProfile : (id) =>
        profiles = @getDataBrowserSettings().getVisualizationProfiles()
        profile = profiles.get id
        
        v = @getVisualizationProfileView()
        v.setProfileToManage profile
        @appState.set mainView : v

      focusOnSearchField : (ev) =>
        @base()
        setTimeout( () -> 
          $("#data-console").val("")
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

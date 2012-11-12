
# TODO: Split into one Module class and one Router class
define(
  ['./search/QueuedSearch',
   './views/DataBrowserView',
   './visualization/views/VisualizationSettingsView',
   './visualization/views/VisualizationProfileView',
   './models/DataBrowserState', 
   './DataBrowserSettings', 
   'neo4j/webadmin/modules/baseui/models/MainMenuModel',
   'ribcage/Router'], 
  (QueuedSearch, DataBrowserView, VisualizationSettingsView, VisualizationProfileView, DataBrowserState, DataBrowserSettings, MainMenuModel, Router) ->

    class DataBrowserRouter extends Router

      routes : 
        "/data/"                                    : "onRoutedToDataURI"
        "/data/visualization/settings/"             : "onRoutedToVisualizationSettings"
        "/data/visualization/settings/profile/"     : "onRoutedToCreateVisualizationProfile"
        "/data/visualization/settings/profile/:id/" : "onRoutedToEditVisualizationProfile"

      shortcuts : 
        "s" : "onEditorFocusShortcut"
        "v" : "onViewTypeToggleShortcut"

      init : (appState) =>
        # Because we need to be able to match newlines, we need to define
        # this route with our own regex
        @route(/data\/search\/([\s\S]*)/i, 'search', @search)

        @appState = appState
        @dataModel = new DataBrowserState( server : @appState.getServer() )
        
        @dataModel.bind "change:query", @onQueryChangedInModel
        @dataModel.bind "change:data", @onDataChangedInModel

        @menuItem = new MainMenuModel.Item 
          title : "Data browser",
          subtitle:"Explore and edit",
          url : @_getCurrentQueryURI()

      search : (query) =>
        @saveLocation()
        query = decodeURIComponent query
        while query.charAt(query.length-1) == "/"
          query = query.substr(0, query.length - 1)

        @dataModel.setQuery query
        @appState.set( mainView : @getDataBrowserView() )

        if @_looksLikeReadOnlyQuery(query)
          @dataModel.executeCurrentQuery()
          
      onRoutedToDataURI : () =>
        # Allows static links to /data/, that route to the current query URI
        @_routeToCurrentQueryURI()

      onRoutedToVisualizationSettings : () =>
        @saveLocation()
        @visualizationSettingsView ?= new VisualizationSettingsView
          dataBrowserSettings : @getDataBrowserSettings()
        @appState.set mainView : @visualizationSettingsView
        
      onRoutedToCreateVisualizationProfile : () =>
        @saveLocation()
        v = @getVisualizationProfileView()
        v.setIsCreateMode(true)
        @appState.set mainView : v
        
      onRoutedToEditVisualizationProfile : (id) =>
        @saveLocation()
        profiles = @getDataBrowserSettings().getVisualizationProfiles()
        profile = profiles.get id
        
        v = @getVisualizationProfileView()
        v.setProfileToManage profile
        @appState.set mainView : v

      #
      # Bootstrapper SPI
      #

      getMenuItems : ->
        [@menuItem]

      # 
      # Event handlers
      # 

      onEditorFocusShortcut : (ev) =>
        @search(@dataModel.getQuery())
        setTimeout( (=> @getDataBrowserView().focusOnEditor()), 1)

      onViewTypeToggleShortcut : (ev) =>
        @getDataBrowserView().switchView()

      onQueryChangedInModel : =>
        url = @_getCurrentQueryURI()

        @menuItem.setUrl(url)

        #if location.hash != url
        #  location.hash = url

      onDataChangedInModel : =>
        @_routeToCurrentQueryURI()

      #
      # Internals
      #

      getDataBrowserView : =>
        @view ?= new DataBrowserView
          state:@appState
          dataModel:@dataModel

      getVisualizationProfileView : =>
        @visualizationProfileView ?= new VisualizationProfileView 
          dataBrowserSettings:@getDataBrowserSettings()
          
      getDataBrowserSettings : ->
        @dataBrowserSettings ?= new DataBrowserSettings @appState.getSettings()

      _routeToCurrentQueryURI : () =>
        url = @_getCurrentQueryURI()

        if location.hash != url
          location.hash = url

      # We only auto-execute read-only queries,
      # and we determine if a query is read-only here.
      # Note: Since we execute queries from the current URL,
      # this is a very real security issue. If modifying queries
      # slip through here, attackers can redirect an adminstrator
      # to a webadmin URL with a malicious Cypher query. Please
      # opt for better-safe-than-sorry when updating this regex.
      _looksLikeReadOnlyQuery : (query) ->
        pattern = ///^(
                    # Super basic cypher queries
                    (start 
                     \s+                     # White space
                     [a-z]+=node\(\d+\)      # Node by id
                     \s+                     # White space
                     return \s+ [a-z]+)      # Return an identifier
                     
                                           | # or
 
                    # Direct node id lookups
                    ((node:)?\d+)          | # or

                    # Direct rel id lookups 
                    (rel:\d+)              | # or

                    # Direct rels for node id lookups
                    (rels:\d+)
                     )$
                  ///i

        pattern.test(query)

      _getCurrentQueryURI : ->
        query = @dataModel.getQuery()
        return "#/data/search/#{encodeURIComponent(query)}/"
)

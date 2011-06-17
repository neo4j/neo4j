
define(
  ['./search/QueuedSearch',
   './views/DataBrowserView',
   './models/DataBrowserState', 
   'ribcage/Router'], 
  (QueuedSearch, DataBrowserView, DataBrowserState, Router) ->

    class DataBrowserRouter extends Router

      routes : 
        "/data/" : "base"
        "/data/search/*query" : "search"

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
)

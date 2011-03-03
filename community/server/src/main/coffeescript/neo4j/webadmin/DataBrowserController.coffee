
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

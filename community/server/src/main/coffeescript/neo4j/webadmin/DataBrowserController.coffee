
define(
  ['neo4j/webadmin/data/QueuedSearch',
   './views/DataBrowserView',
   './models/DataBrowserState', 
   'lib/backbone'], 
  (QueuedSearch, DataBrowserView, DataBrowserState) ->

    class DataBrowserController extends Backbone.Controller
      routes : 
        "/data/" : "base"
        "/data/search/*query" : "search"

      initialize : (appState) =>
        @appState = appState
        @server = appState.get "server"
        @searcher = new QueuedSearch(@server)

        @dataModel = new DataBrowserState( server : @server )

        @dataModel.bind "change:query", @queryChanged

      base : =>
        @queryChanged()

      search : (query) =>
        while query.charAt(query.length-1) == "/"
          query = query.substr(0, query.length - 1)

        @dataModel.setQuery query
        @appState.set( mainView : @getDataBrowserView() )

      queryChanged : =>
        query = @dataModel.get "query"
        if query == null
          return @search("0")

        url = "#/data/search/#{query}/"

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

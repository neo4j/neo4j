
define(
  ['./views/DataBrowserView', 
   './models/DataBrowserState', 
   './models/DataItem', 'lib/backbone'], 
  (DataBrowserView, DataBrowserState, DataItem) ->
  
    class DataBrowserController extends Backbone.Controller
      routes : 
        "/data/" : "base",
        "/data/node/:id" : "node",
        "/data/relationship/:id" : "relationship",

      initialize : (appState) =>
        @appState = appState
        @server = appState.get "server"
        @dataModel = new DataBrowserState( server : @server )

      base : =>
        @appState.set( mainView : @getDataBrowserView() )

      node : (id) =>
        @base()
        @server.node(@nodeUri(id)).then(@showNode, @showNotFound)

      relationship : (id) =>
        @base()
        @server.rel(@relationshipUri(id)).then(@showRelationship, @showNotFound)


      showNode : (node) =>
        @dataModel.set({"data":node, type:"node"})

      showRelationship : (relationship) =>
        @dataModel.set({"data":relationship, type:"relationship"})
     
      showNotFound : =>
        @dataModel.set({"data":null, type:"not-found"})


      nodeUri : (id) ->
        @server.url + "/db/data/node/" + id

      relationshipUri : (id) ->
        @server.url + "/db/data/relationship/" + id

      getDataBrowserView : =>
        @dataBrowserView ?= new DataBrowserView({state:@appState, dataModel:@dataModel})
)

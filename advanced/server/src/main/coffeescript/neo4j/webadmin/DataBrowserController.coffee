
define(
  ['./views/DataBrowserView', 
   './models/DataBrowserModel', 
   './models/DataItemModel', 'lib/backbone'], 
  (DataBrowserView, DataBrowserModel, DataItemModel) ->
  
    class DataBrowserController extends Backbone.Controller
      routes : 
        "/data/" : "base",
        "/data/node/:id" : "node",
        "/data/relationship/:id" : "relationship",

      initialize : (appState) =>
        @appState = appState
        @server = appState.get "server"
        @dataModel = new DataBrowserModel( server : @server )

      base : =>
        @appState.set( mainView : @getDataBrowserView() )

      node : (id) =>
        @base()
        @server.node(@nodeUri(id)).then(@showNode, @showNotFound)

      relationship : (id) =>
        @base()
        @server.rel(@relationshipUri(id)).then(@showRelationship, @showNotFound)


      showNode : (node) =>
        console.log node
        @dataModel.set({"item":new DataItemModel({item:node, type:"node"})})

      showRelationship : (relationship) =>
        @dataModel.set({"item":new DataItemModel({item:relationship, type:"relationship"})})
     
      showNotFound : =>
        @dataModel.set({"item":new DataItemModel({item:null, type:"not-found"})})


      nodeUri : (id) ->
        @server.url + "/db/data/node/" + id

      relationshipUri : (id) ->
        @server.url + "/db/data/relationship/" + id

      getDataBrowserView : =>
        @dataBrowserView ?= new DataBrowserView({state:@appState, dataModel:@dataModel})
)

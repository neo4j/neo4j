
define( 
  ['./models/ServerInfo', 
   './views/ServerInfoView', 
   'lib/backbone'], 
  (ServerInfo, ServerInfoView) ->
    
    class ServerInfoController extends Backbone.Controller
      routes : 
        "/info/" : "base",
        "/info/:domain/:name/" : "bean",

      initialize : (appState) =>
        @appState = appState
        @serverInfo = new ServerInfo { server : @appState.get "server" } 
        @server = @appState.get "server"

      base : =>
        @appState.set( mainView : @getServerInfoView() )
        @serverInfo.fetch()

      bean : (domain, name) => 
        @appState.set( mainView : @getServerInfoView() )
        @serverInfo.setCurrent(domain, name)

      getServerInfoView : =>
        @serverInfoView ?= new ServerInfoView( {appState:@appState, serverInfo:@serverInfo} )
)

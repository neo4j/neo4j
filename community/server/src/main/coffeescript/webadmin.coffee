
require(
  ["neo4j/webadmin/DashboardController"
   "neo4j/webadmin/DataBrowserController"
   "neo4j/webadmin/ConsoleController"
   "neo4j/webadmin/ServerInfoController"
   "neo4j/webadmin/models/ApplicationState"
   "neo4j/webadmin/views/BaseView"
   "neo4j/webadmin/ui/FoldoutWatcher"
   "lib/neo4js", "lib/jquery", "lib/underscore", "lib/backbone"]
  (DashboardController, DataBrowserController, ConsoleController, ServerInfoController, ApplicationState, BaseView, FoldoutWatcher) ->
    
    appState = new ApplicationState
    appState.set server : new neo4j.GraphDatabase(location.protocol + "//" + location.host)

    new BaseView(el:$("body"),appState:appState)

    new DashboardController appState
    new DataBrowserController appState
    new ConsoleController appState
    new ServerInfoController appState

    foldoutWatcher = new FoldoutWatcher
    foldoutWatcher.init()

    Backbone.history.start()
)

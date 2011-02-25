(function() {
  require(["neo4j/webadmin/DashboardController", "neo4j/webadmin/DataBrowserController", "neo4j/webadmin/ConsoleController", "neo4j/webadmin/ServerInfoController", "neo4j/webadmin/models/ApplicationState", "neo4j/webadmin/views/BaseView", "lib/jquery", "lib/underscore", "lib/backbone"], function(DashboardController, DataBrowserController, ConsoleController, ServerInfoController, ApplicationState, BaseView) {
    var appState, baseview;
    $(document).ready(function() {});
    appState = new ApplicationState;
    baseview = new BaseView({
      el: $("body"),
      appState: appState
    });
    baseview.render();
    new DashboardController(appState);
    new DataBrowserController(appState);
    new ConsoleController(appState);
    new ServerInfoController(appState);
    return Backbone.history.start();
  });
}).call(this);

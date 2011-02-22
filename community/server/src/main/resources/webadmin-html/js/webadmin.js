(function() {
  require(['neo4j/webadmin/DashboardController', 'neo4j/webadmin/views/BaseView', 'lib/jquery', 'lib/underscore', 'lib/backbone'], function(DashboardController, BaseView) {
    return $(document).ready(function() {
      var baseview;
      baseview = new BaseView({
        el: $("body")
      });
      baseview.render();
      new DashboardController;
      return Backbone.history.start();
    });
  });
}).call(this);

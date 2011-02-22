
require ['neo4j/webadmin/DashboardController','neo4j/webadmin/views/BaseView', 'lib/jquery', 'lib/underscore','lib/backbone'], 
  (DashboardController, BaseView) ->
    $(document).ready ->
    
      baseview = new BaseView(el:$("body"))
      baseview.render()
    
      new DashboardController
      
      Backbone.history.start()
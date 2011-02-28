
define( 
  ['./views/DashboardView'
   './models/ServerPrimitives'
   './models/DiskUsage'
   './models/CacheUsage'
   'lib/backbone'],
  (DashboardView, ServerPrimitives, DiskUsage, CacheUsage) ->
  
    class DashboardController extends Backbone.Controller
      routes : 
        "" : "dashboard"

      initialize : (appState) =>
        @appState = appState

      dashboard : =>
        @appState.set( mainView : @getDashboardView() )

      getDashboardView : =>
        @dashboardView ?= new DashboardView  
          state      : @appState
          primitives : @getServerPrimitives()
          diskUsage  : @getDiskUsage()
          cacheUsage : @getCacheUsage()

      getServerPrimitives : =>
        @serverPrimitives ?= new ServerPrimitives( server : @appState.getServer(), pollingInterval : 2000 )

      getDiskUsage : =>
        @diskUsage ?= new DiskUsage( server : @appState.getServer(), pollingInterval : 2000 )

      getCacheUsage : =>
        @cacheUsage ?= new CacheUsage( server : @appState.getServer(), pollingInterval : 2000 )

)

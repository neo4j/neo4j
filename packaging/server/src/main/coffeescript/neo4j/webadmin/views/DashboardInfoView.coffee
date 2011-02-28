define ['neo4j/webadmin/templates/dashboard_info','lib/backbone'], (template) ->
  
  class DashboardInfoView extends Backbone.View
    
    template : template
   
    initialize : (opts) =>
      console.log(opts)
      @primitives = opts.primitives
      @diskUsage = opts.diskUsage
      @cacheUsage = opts.cacheUsage
      
      @primitives.bind("change",@render)
      @diskUsage.bind("change",@render)
      @cacheUsage.bind("change",@render)

    render : =>
      $(@el).html @template
        primitives : @primitives
        diskUsage  : @diskUsage
        cacheUsage : @cacheUsage
      return this

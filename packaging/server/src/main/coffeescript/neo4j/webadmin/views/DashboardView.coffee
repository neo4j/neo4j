define( 
  ['neo4j/webadmin/templates/dashboard',
   './DashboardInfoView',        
   'lib/backbone'], 
  (template, DashboardInfoView) ->

    class DashboardView extends Backbone.View
      
      template : template
     
      initialize : (opts) =>
        @appState = opts.state
        @infoView = new DashboardInfoView(opts)

      render : =>
        $(@el).html @template(
          server : { url : "someurl", version : "someversion" } )
        $("#dashboard-info", @el).append(@infoView.render().el)
        return this
)

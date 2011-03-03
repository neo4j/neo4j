define(
  ['neo4j/webadmin/data/Search',
   'neo4j/webadmin/data/ItemUrlResolver',
   './databrowser/SimpleView',
   'neo4j/webadmin/templates/data/base','lib/backbone'], 
  (Search, ItemUrlResolver, SimpleView, template) ->

    class DataBrowserView extends Backbone.View
      
      template : template

      events : 
        "keyup #data-console" : "search"
        "click #data-create-node" : "createNode"

      initialize : (options)->
        @dataModel = options.dataModel
        @server = options.state.getServer()
        @urlResolver = new ItemUrlResolver(@server)
        @dataView = new SimpleView(dataModel:options.dataModel)

      render : ->
        $(@el).html(@template())
        $("#data-area", @el).append @dataView.el 
        return this

      search : (ev) =>
        @dataModel.setQuery( $("#data-console",@el).val() )

      createNode : =>
        @server.node({}).then (node) =>
          id = @urlResolver.extractNodeId(node.getSelf())
          @dataModel.setData( node, true, {silent:true} ) 
          @dataModel.setQuery( id, true) 
)

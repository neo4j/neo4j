define(
  ['./NodeView',
   './RelationshipView',
   './ListView',
   'neo4j/webadmin/templates/databrowser/notfound',
   'lib/backbone'], 
  (NodeView, RelationshipView, ListView, notFoundTemplate) ->
  
    class SimpleView extends Backbone.View

      initialize : (options)->
        
        @nodeView = new NodeView
        @relationshipView = new RelationshipView
        @listView = new ListView

        @dataModel = options.dataModel
        @dataModel.bind("change", @render)

      render : =>
        type = @dataModel.get("type")
        switch type
          when "node"
            view = @nodeView
          when "relationship"
            view = @relationshipView
          when "set"
            view = @listView
          else
            $(@el).html(notFoundTemplate())
            return this
        view.setDataModel(@dataModel)
        $(@el).html(view.render().el)

)

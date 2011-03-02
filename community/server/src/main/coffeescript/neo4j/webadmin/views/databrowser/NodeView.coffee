define(
  ['neo4j/webadmin/templates/databrowser/node',
   './PropertyContainerView','lib/backbone'], 
  (template, PropertyContainerView) ->
  
    class NodeView extends PropertyContainerView

      initialize : (opts={}) =>
        opts.template = template
        super(opts)

)

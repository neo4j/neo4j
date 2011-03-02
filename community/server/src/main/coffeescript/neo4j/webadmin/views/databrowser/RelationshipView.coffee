define(
  ['neo4j/webadmin/templates/data/relationship',
   './PropertyContainerView','lib/backbone'], 
  (template, PropertyContainerView) ->
  
    class RelationshipView extends PropertyContainerView

      initialize : (opts={}) =>
        opts.template = template
        super(opts)

)


define(
  ['./StyleRules',
   'ribcage/LocalModel'], 
  (StyleRules, LocalModel) ->

    class VisualizationProfile extends LocalModel
      
      initialize : () ->
        @initNestedCollection('styleRules', StyleRules)
      
      setName : (name) -> @set name:name
      getName : () -> @get "name"
        
      isBuiltin : () -> @get "builtin"
      
      # Given a visualization node, 
      # apply appropriate style attributes
      styleNode : (visualNode) ->
        @styleRules.each (rule) =>
          if rule.appliesTo visualNode, 'node'
            rule.applyStyleTo visualNode
      
)

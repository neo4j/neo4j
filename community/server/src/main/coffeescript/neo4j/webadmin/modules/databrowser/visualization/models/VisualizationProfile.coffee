
define(
  ['./StyleRules',
   './NodeStyle',
   'ribcage/LocalModel'], 
  (StyleRules, NodeStyle, LocalModel) ->

    class VisualizationProfile extends LocalModel
      
      initialize : () ->
        @initNestedModel('styleRules', StyleRules)
        @_defaultNodeStyle = new NodeStyle
      
      setName : (name) -> @set name:name
      getName : () -> @get "name"
        
      isBuiltin : () -> @get "builtin"
      
      # Given a visualization node, 
      # apply appropriate style attributes
      styleNode : (visualNode) ->
        @_defaultNodeStyle.applyTo visualNode
        @styleRules.each (rule) =>
          if rule.appliesTo visualNode, 'node'
            rule.applyStyleTo visualNode
            console.log visualNode.style
      
)

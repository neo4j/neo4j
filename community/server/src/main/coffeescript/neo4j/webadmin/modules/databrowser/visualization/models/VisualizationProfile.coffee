
define(
  ['./StyleRules',
   './style',
   'ribcage/LocalModel'], 
  (StyleRules, style, LocalModel) ->

    class VisualizationProfile extends LocalModel
      
      initialize : () ->
        @initNestedModel('styleRules', StyleRules)
        @_defaultNodeStyle = new style.NodeStyle
        @_defaultGroupStyle = new style.GroupStyle
      
      setName : (name) -> @set name:name
      getName : () -> @get "name"
        
      isDefault : () -> @get "builtin"
      
      # Given a visualization node, 
      # apply appropriate style attributes
      styleNode : (visualNode) ->
        
        type = if visualNode.type is "group" then "group" else "node"
        
        switch type
          when "group" then @_defaultGroupStyle.applyTo visualNode
          when "node" then @_defaultNodeStyle.applyTo visualNode
        
        rules = @styleRules.models
        for i in [rules.length-1..0] by -1
          rule = rules[i]
          if rule.appliesTo visualNode, type
            rule.applyStyleTo visualNode
            
        if visualNode.type is "unexplored"
          visualNode.style.shapeStyle.alpha = 0.2
      
)

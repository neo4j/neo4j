
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
      
)

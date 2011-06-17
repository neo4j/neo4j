
define(
  [], 
  () ->

    class DataBrowserSettings

      LABEL_PROPERTIES_KEY : 'databrowser.labelProperties'
      LABEL_PROPERTIES_DEFAULT : ['name']

      constructor : (@settings) ->
        # Pass

      getLabelProperties : () ->
        @settings.get(@LABEL_PROPERTIES_KEY) or @LABEL_PROPERTIES_DEFAULT
        
      setLabelProperties : (properties) ->
        attr = {}
        attr[@LABEL_PROPERTIES_KEY] = properties
        @settings.set(attr)
        @settings.save()
      
      labelPropertiesChanged : (callback) ->
        @settings.bind 'change:' + @LABEL_PROPERTIES_KEY,  callback
      
)

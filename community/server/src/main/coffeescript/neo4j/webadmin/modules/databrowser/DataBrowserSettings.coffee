
define(
  [], 
  () ->

    class DataBrowserSettings

      LABEL_PROPERTIES_KEY : 'databrowser.labelProperties'
      LABEL_PROPERTIES_DEFAULT : ['name']

      constructor : (@settings) ->
        # Pass

      getLabelProperties : () ->
        s = @settings.get(@LABEL_PROPERTIES_KEY)
        if s and _(s).isArray()
          s
        else @LABEL_PROPERTIES_DEFAULT
        
      setLabelProperties : (properties) ->
        attr = {}
        attr[@LABEL_PROPERTIES_KEY] = properties
        @settings.set(attr)
        @settings.save()
      
      labelPropertiesChanged : (callback) ->
        @settings.bind 'change:' + @LABEL_PROPERTIES_KEY,  callback
      
)

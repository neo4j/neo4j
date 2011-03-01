define(
  ['neo4j/webadmin/templates/server_info',
   'neo4j/webadmin/templates/server_info_bean',
   'lib/backbone'], 
  (baseTemplate, beanTemplate) ->
  
    class ServerInfoView extends Backbone.View
      
      initialize : (options) ->
        @serverInfo = options.serverInfo

        @baseTemplate = baseTemplate
        @beanTemplate = beanTemplate

        @serverInfo.bind "change:domains", @render
        @serverInfo.bind "change:current", @renderBean

      render : =>
        $(@el).html @baseTemplate( { domains : @serverInfo.get("domains") } )
        @renderBean()
        return this

      renderBean : =>
        bean = @serverInfo.get("current")
        $("#info-bean", @el).empty().append @beanTemplate(
          bean : bean,
          attributes : if bean? then @flattenAttributes(bean.attributes) else [])
        return this

      flattenAttributes: (attributes, flattened=[], indent=1) =>
        for attr in attributes
          name = if attr.name? then attr.name else if attr.type? then attr.type else ""
          
          pushedAttr =
            name : name,
            description : attr.description,
            indent : indent
          flattened.push pushedAttr

          if not attr.value?
            pushedAttr.value = ""
          else if _(attr.value).isArray() and _(attr.value[0]).isString()
            pushedAttr.value = attr.value.join(", ") 
          else if _(attr.value).isArray()
            pushedAttr.value = ""
            @flattenAttributes(attr.value, flattened, indent + 1)
          else if typeof(attr.value) is "object"
            pushedAttr.value = ""
            @flattenAttributes(attr.value.value, flattened, indent + 1)
          else
            pushedAttr.value = attr.value

        return flattened
)

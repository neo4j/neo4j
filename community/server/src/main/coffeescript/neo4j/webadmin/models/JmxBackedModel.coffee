
define ['lib/backbone'], () ->
  
  class JmxBackedModel extends Backbone.Model
    
    initialize : (options) =>
      @server = options.server
      @jmx = @server.manage.jmx

      if options.pollingInterval? and options.pollingInterval > 0
        @fetch()
        @setPollingInterval options.pollingInterval

    setPollingInterval : (ms) =>
      if @interval?
        clearInterval(@interval)
      
      @interval = setInterval(@fetch, ms)

    fetch : =>
      parseBean = @parseBean
      for key, def of @beans
        @jmx.getBean def.domain, def.name, (bean) ->
          parseBean(key, bean)

    parseBean : (key, bean) =>
      if bean?
        values = {}
        for attribute in bean.attributes
          values[attribute.name] = attribute.value

        @set(values)

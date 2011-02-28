

define ['lib/backbone'], () ->
  
  class ServerInfo extends Backbone.Model

    initialize : (opts) =>
      @server = opts.server

    setCurrent : (domain, beanName) =>
      @current = {domain:domain, beanName:beanName}
      
      if @get("domains")? and @getBean(domain,beanName)?
        @set current:@getBean(domain,beanName)
      else
        @fetch()

    fetch : =>
      @server.manage.jmx.query ["*:*"], @parseJmxBeans

    parseJmxBeans : (beans) =>
      NEO4J_DOMAIN = "org.neo4j"
      beans = beans.sort (a,b) ->
        aName = if a.domain is NEO4J_DOMAIN then "0" + a.getName() else a.jmxName
        bName = if b.domain is NEO4J_DOMAIN then "0" + b.getName() else b.jmxName
        return aName.toLowerCase() > bName.toLowerCase()

      domains = []
      currentDomainName = null
      currentDomainBeans = []
      for bean in beans
        if currentDomainName != bean.domain
          currentDomainName = bean.domain
          currentDomainBeans = []
          domains.push { name:bean.domain, beans:currentDomainBeans }
        currentDomainBeans.push bean

        @setBean(bean)
        if @current? and @current.domain is currentDomainName and @current.beanName is bean.getName()
          @set {current : bean }, {silent:true}

      if not @current? and domains.length > 0 and domains[0].beans.length > 0
        @set {current : domains[0].beans[0] }, {silent:true}

      @set { domains : domains }

    setBean : (bean) =>
      beanData = {}
      beanData["#{bean.domain}:#{bean.getName()}"] = bean
      @set beanData, {silent:true}

    getBean : (domain, name) =>
      @get("#{domain}:#{name}")

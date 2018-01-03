###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define ['ribcage/Model'], (Model) ->
  
  class ServerInfo extends Model

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
    
      for bean in beans
        names = []
        for k,v of bean.properties
          if k[0...4] is 'name' then names.push v
        bean.name = names.join(' - ')
        if bean.name.length == 0 then bean.name = bean.jmxName
      
      NEO4J_DOMAIN = "org.neo4j"
      beans = beans.sort (a,b) ->
        aName = if a.domain is NEO4J_DOMAIN then "000" + a.name else a.jmxName
        bName = if b.domain is NEO4J_DOMAIN then "000" + b.name else b.jmxName
        aGreaterThanB = aName.toLowerCase() > bName.toLowerCase()
        if aGreaterThanB == true
          return 1
        else if aGreaterThanB == false
          return -1
        else
          return aGreaterThanB

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
        if @current? and @current.domain is currentDomainName and @current.beanName is bean.name
          @set {current : bean }, {silent:true}

      if not @current? and domains.length > 0 and domains[0].beans.length > 0
        @set {current : domains[0].beans[0] }, {silent:true}

      @set { domains : domains }

    setBean : (bean) =>
      beanData = {}
      beanData["#{bean.domain}:#{bean.name}"] = bean
      @set beanData, {silent:true}

    getBean : (domain, name) =>
      @get("#{domain}:#{name}")

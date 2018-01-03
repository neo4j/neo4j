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
  
  class JmxBackedModel extends Model
    
    initialize : (options) =>
      @server = options.server
      @jmx = @server.manage.jmx
      @dataAvailable = false

      if options.pollingInterval? and options.pollingInterval > 0
        @fetch()
        @setPollingInterval options.pollingInterval

    isDataAvailable : () ->
      @dataAvailable

    setPollingInterval : (ms) =>
      if @interval?
        clearInterval(@interval)
      
      @interval = setInterval(@fetch, ms)

    fetch : =>
      for key, def of @beans
        @jmx.getBean def.domain, def.name, @beanParser(key)

    beanParser : (key) ->
      (bean) =>
        if bean? and bean.attributes?
          @setBeanDataAvailable key
          values = {}
          for attribute in bean.attributes
            values[attribute.name] = attribute.value

          update = {}
          update[key] = values
          @set(update)
    
    setBeanDataAvailable : (key) ->
      @beans[key].dataAvailable = true
      dataAvailable = true
      for k,b of @beans
        if not b.dataAvailable
          dataAvailable = false
          break            
      @dataAvailable = dataAvailable

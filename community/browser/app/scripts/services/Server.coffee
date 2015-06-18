###!
Copyright (c) 2002-2015 "Neo Technology,"
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

'use strict';

angular.module('neo4jApp.services')
  .factory 'Server', [
    '$http'
    '$q'
    'Settings'
    ($http, $q, Settings) ->

      # http://docs.angularjs.org/api/ng.$http
      httpOptions =
        timeout: (Settings.maxExecutionTime * 1000)

      returnAndUpdate = (Type, promise) ->
        rv = new Type()
        promise.success(
          (r) ->
            if angular.isArray(rv)
              rv.push.apply(rv, r)
            else
              angular.extend(rv, r)
        )
        rv

      returnAndUpdateArray = (promise) -> returnAndUpdate(Array, promise)

      returnAndUpdateObject = (promise) -> returnAndUpdate(Object, promise)

      class Server
        constructor: ->

        #
        # Basic HTTP methods
        #
        options: (path = '', options = {}) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          options.method = 'OPTIONS'
          options.url = path
          $http(options)

        head: (path = '', options) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          $http.head(path, options or httpOptions)

        delete: (path = '', data = null) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          $http.delete(path, httpOptions)

        get: (path = '', options) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          $http.get(path, options or httpOptions)

        post: (path = '', data) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          $http.post(path, data, httpOptions)

        put: (path = '', data) ->
          path = Settings.host + path unless path.indexOf(Settings.host) is 0
          $http.put(path, data, httpOptions)

        transaction: (opts) ->
          opts = angular.extend(
            path: '',
            statements: [],
            method: 'post'
          , opts)
          {path, statements, method} = opts
          path = Settings.endpoint.transaction + path
          method = method.toLowerCase()
          for s in statements
            s.resultDataContents = ['row','graph']
            s.includeStats = true
          @[method]?(path, {statements: statements})

        #
        # Convenience methods
        #

        # WARNING: avoid using this, as the raw shell is to be deprecated
        console: (command, engine = "shell") ->
          @post(Settings.endpoint.console, {command: command, engine: engine})

        # one-shot cypher queries
        cypher: (path = '', data) ->
          @post("#{Settings.endpoint.cypher}" + path, data)

        # JMX queries
        jmx: (query) ->
          @post Settings.endpoint.jmx, query

        labels: ->
          returnAndUpdateArray @get Settings.endpoint.rest + '/labels'

        relationships: ->
          returnAndUpdateArray @get Settings.endpoint.rest + '/relationship/types'

        propertyKeys: ->
          returnAndUpdateArray @get Settings.endpoint.rest + '/propertykeys'

        info: ->
          returnAndUpdateObject @get Settings.endpoint.rest + '/'

        version: ->
          returnAndUpdateObject @get Settings.endpoint.version + '/'

        status: (params = '')->
          # User a smaller timeout for status requests so IE10 detects when the
          # server goes down faster.
          @options "#{Settings.endpoint.rest}/", { timeout: (Settings.heartbeat * 1000)}

        log: (path) ->
          @get(path).then((r)-> console.log (r))

        hasData: ->
          q = $q.defer()
          @cypher('?profile=true', {query: "MATCH (n) RETURN ID(n) LIMIT 1"})
          .success((r) -> q.resolve(r.plan.rows == 1))
          .error(q.reject)
          q.promise

      new Server
  ]

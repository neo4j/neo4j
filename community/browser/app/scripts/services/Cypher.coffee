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
  .factory 'Cypher', [
    '$q'
    '$rootScope'
    'Server'
    ($q, $rootScope, Server) ->
      parseId = (resource = "") ->
        id = resource.split('/').slice(-2, -1)
        return parseInt(id, 10)

      class CypherResult
        constructor: (@_response = {}) ->
          @nodes = []
          @other = []
          @relationships = []
          @size = 0
          @stats = {}

          @size = @_response.data?.length or 0

          if @_response.stats
            @_setStats @_response.stats

          @_response.data ?= []
          return @_response unless @_response.data?
          for row in @_response.data
            for node in row.graph.nodes
              @nodes.push node
            for relationship in row.graph.relationships
              @relationships.push relationship

          @_response

        response: -> @_response

        rows: ->
          # TODO: Maybe cache rows
          for entry in @_response.data
            for cell in entry.row
              if not (cell?)
                null
              else if cell.self?
                angular.copy(cell.data)
              else
                angular.copy(cell)

        columns: ->
          @_response.columns

        # Tell wether the result is pure text (ie. no nodes or relations)
        isTextOnly: ->
          @nodes.length is 0 and @relationships.length is 0

        _setStats: (@stats) ->
          return unless @stats?
          if stats.labels_added > 0 or stats.labels_removed > 0
            $rootScope.$broadcast 'db:changed:labels', angular.copy(@stats)

      promiseResult = (promise) ->
        q = $q.defer()
        promise.success(
          (result) =>
            if not result
              q.reject()
            else if result.errors.length > 0
              q.reject(result.errors)
            else
              results = []
              for r in result.results
                results.push( new CypherResult(r) )
              q.resolve( results[0] ) # TODO: handle multiple...
        ).error(q.reject)
        q.promise

      class CypherTransaction
        constructor: () ->
          @_reset()

        _onSuccess: () ->

        _onError: () ->

        _reset: ->
          @id = null

        # TODO: the @id is assigned in a promise, we should probably keep the promise around to do the right thing
        #       if the id hasn't been assigned yet, but the request is still running.
        begin: (query) ->
          statements = if query then [{statement:query}] else []
          q = $q.defer()
          Server.transaction(
            path: ""
            statements: statements
          ).success(
            (r) =>
              @id = parseId(r.data.commit)
              q.resolve(r)
            (r) => q.reject(r)
          )
          promiseResult(q.promise)

        execute: (query) ->
          return @begin(query) unless @id
          promiseResult(Server.transaction(
            path: '/' + @id
            statements: [
              statement: query
            ]
          ))

        commit: (query) ->
          statements = if query then [{statement:query}] else []
          if @id
            q = $q.defer()
            Server.transaction(
              path: "/#{@id}/commit"
              statements: statements
            ).success(
              (r) =>
                @_reset()
                q.resolve(r)
              (r) => q.reject(r)
            )
            promiseResult(q.promise)
          else
            promiseResult(Server.transaction(
              path: "/commit"
              statements: statements
            ))

        # FIXME: What is wrong?
        # DELETE http://localhost:7474/db/data/transaction/14 415 (Unsupported Media Type)
        rollback: ->
          return unless @id
          Server.transaction(
            method: 'DELETE'
            path: '/' + @id
          ).success(=> @_reset())

      class CypherService
        profile: (query) ->
          q = $q.defer()
          Server.cypher('?profile=true', {query: query})
          .success((r) -> q.resolve(r.plan))
          .error(q.reject)
          q.promise

        send: (query) -> # Deprecated
          @transaction().commit(query)

        transaction: ->
          new CypherTransaction()

      window.Cypher = new CypherService()
]


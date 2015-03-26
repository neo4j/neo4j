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
  .service 'UsageDataCollectionService', [
    'Settings'
    'localStorageService'
    'Intercom'
    '$timeout'
    (Settings, localStorageService, Intercom, $timeout) ->
      storageKey = 'udc'

      class UsageDataCollectionService
        constructor: ->
          @data = localStorageService.get(storageKey)
          @data = @reset() unless angular.isObject(@data)
          @data.client_starts = (@data.client_starts || 0) + 1
          @save()

        reset: ->
          @data = {
            uuid: UUID.genV1().toString()
            created_at: Math.round(Date.now() / 1000)
            client_starts: 0
          }
          @save()
          return @data

        save: ->
          localStorageService.set(storageKey, JSON.stringify(@data))

        set: (key, value) ->
          @data[key] = value
          @save()
          return value

        increment: (key) ->
          @data[key] = (@data[key] || 0) + 1
          @save()
          return @data[key]

        ping: (event) ->
          if (@shouldPing(event))
            switch event
              when 'connect'
                @connectUser()
                Intercom.update({
                  "companies": [
                      {
                        type: "company"
                        name: "Neo4j " + @data.neo4j_version + " " + @data.store_id
                        company_id: @data.store_id
                        # custom_attibutes: {
                        #   neo4j_version: @data.neo4j_version
                        # }
                      }
                    ]
                  })
                Intercom.event('connect', {
                    store_id: @data.store_id
                    neo4j_version: @data.neo4j_version
                    client_starts: @data.client_starts
                    cypher_attempts: @data.cypher_attempts
                    cypher_wins: @data.cypher_wins
                    cypher_fails: @data.cypher_fails
                    accepts_replies: Settings.acceptsReplies
                  })

        connectUser: ->
          @data.name = Settings.userName
          Intercom.user(@data.uuid, @data)

        pingLater: (event) =>
          timer = $timeout(
            () =>
              @ping(event)
            ,
            (Settings.heartbeat * 1000)
          )

        shouldPing: (event) =>
          if not (Settings.shouldReportUdc?)
            @pingLater(event)
            return false
          if not @hasRequiredData()
            @pingLater(event)
            return false
          if Settings.shouldReportUdc
            pingTime = new Date(@data.pingTime || 0)
            today = new Date()
            today = new Date(today.getFullYear(), today.getMonth(), today.getDay())

            if (pingTime < today)
              @set("pingTime", today.getTime())
              return true
            else
              return false
          else
            return false

        hasRequiredData: ->
          return @data.store_id and @data.neo4j_version

        toggleMessenger: ->
          @connectUser()
          Intercom.toggle()

        newMessage: (message) ->
          @connectUser()
          Intercom.newMessage message

      new UsageDataCollectionService()
  ]

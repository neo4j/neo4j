###!
Copyright (c) 2002-2014 "Neo Technology,"
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
          Intercom.user(@data.uuid, @data)

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
                Intercom.update({
                  "companies": [
                      {
                        type: "company"
                        name: "Neo4j " + @data.store_id
                        company_id: @data.store_id
                      }
                    ]
                  })
                Intercom.event('connect', {
                    store_id: @data.store_id
                  })

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
          if Settings.shouldReportUdc
            pingTime = new Date(@data.pingTime || 0)
            today = new Date()
            today = new Date(today.getFullYear(), today.getMonth(), today.getDay())

            if (pingTime < today) || true
              @set("pingTime", today.getTime())
              return true
            else
              return false
          else
            return false




      new UsageDataCollectionService()
  ]

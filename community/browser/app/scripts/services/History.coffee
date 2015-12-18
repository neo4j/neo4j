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
  .service 'HistoryService', [
    'Settings'
    'localStorageService'
    (Settings, localStorageService) ->
      storageKey = 'history'
      class HistoryService
        constructor: ->
          @history = localStorageService.get(storageKey)
          @history = [] unless angular.isArray(@history)
          @current = ''
          @cursor = -1

        add: (input) ->
          @current = ''
          if input?.length > 0 and @history[0] isnt input
            @history.unshift(input)
            @history.pop() until @history.length <= Settings.maxHistory
            localStorageService.set(storageKey, JSON.stringify(@history))
          @get(-1)

        next: ->
          idx = @cursor
          idx ?= @history.length
          idx--
          @get(idx)

        prev: ->
          idx = @cursor
          idx ?= -1
          idx++
          @get(idx)

        setBuffer: (input) ->
          @buffer = input

        get: (idx) ->
          # cache unsaved changes if moving away from the temporary buffer
          @current = @buffer if @cursor == -1 and idx != -1

          idx = -1 if idx < 0
          idx = @history.length - 1 if idx >= @history.length
          @cursor = idx
          @history[idx] or @current

      new HistoryService()
  ]

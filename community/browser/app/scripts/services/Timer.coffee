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
  .factory 'Timer', () ->

    currentTime = -> (new Date).getTime()

    class Timer
      constructor: ->
        @_start = null
        @_end = null

      isRunning: ->
        return @_start?

      start: ->
        @_start ?= currentTime()
        @

      started: -> @_start

      stop: ->
        @_end ?= currentTime()
        @

      stopped: -> @_end

      time: ->
        return 0 unless @_start?
        end = @_end or currentTime()
        end - @_start

    class TimerService
      timers = {}

      constructor: ->

      new: (name = 'default') ->
        timers[name] = new Timer()

      start: (name = 'default') ->
        timer = @new(name)
        timer.start()

      stop: (name = 'default') ->
        return undefined unless timers[name]?
        timers[name].stop()

      currentTime: currentTime


    new TimerService

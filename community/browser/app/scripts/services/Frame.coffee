###!
Copyright (c) 2002-2013 "Neo Technology,"
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
.provider 'Frame', [
  ->
    self = @
    @interpreters = []

    @$get = [
      '$injector'
      '$q'
      'Collection'
      'Settings'
      'Timer'
      'Utils'
      ($injector, $q, Collection, Settings, Timer, Utils) ->
        class Frame
          constructor: (data = {})->
            @templateUrl = null
            if angular.isString(data)
              @input = data
            else
              angular.extend(@, data)
            @id ?= UUID.genV1().toString()

          toJSON: ->
            {@id, @input}

          exec: ->
            query = Utils.stripComments(@input.trim())
            return unless query
            # Find first matching input interpretator
            intr = frames.interpreterFor(query)
            return unless intr
            @type = intr.type
            intrFn = $injector.invoke(intr.exec)

            @setProperties()

            @errorText = no
            @detailedErrorText = no
            @hasErrors = no
            @isLoading = yes
            @response  = null
            @templateUrl = intr.templateUrl
            timer = Timer.start()
            @startTime = timer.started()
            $q.when(intrFn(query, $q.defer())).then(
              (result) =>
                @isLoading = no
                @response = result
                @runTime = timer.stop().time()
              ,
              (result = {}) =>
                @isLoading = no
                @hasErrors = yes
                @response = null
                @errorText = result.message or "Unknown error"
                if result.length > 0 and result[0].code
                  @errorText = result[0].code
                  @detailedErrorText = result[0].message if result[0].message
                @runTime = timer.stop().time()
            )
            @
          setProperties: ->
            # FIXME: this should maybe be defined by the interpreters
            @exportable     = @type in ['cypher', 'http']
            @fullscreenable = yes


        class Frames extends Collection
          create: (data = {})  ->
            return unless data.input
            intr = @interpreterFor(data.input)
            return undefined unless intr
            if intr.templateUrl
              frame = new Frame(data)
            else
              rv = $injector.invoke(intr.exec)()

            if frame
              # Make sure we don't create more frames than allowed
              @add(frame.exec())
              @remove(@first()) until @length <= Settings.maxFrames
            frame or rv

          interpreterFor: (input = '') ->
            intr = null
            input = Utils.stripComments(input.trim())
            firstWord = Utils.firstWord(input).toLowerCase()
            for i in self.interpreters
              if angular.isFunction(i.matches)
                if i.matches(input)
                  return i
              else
                cmds = i.matches
                cmds = [cmds] if angular.isString(i.matches)
                if angular.isArray(cmds)
                  if cmds.indexOf(firstWord) >= 0
                    return i
                    we
            intr

          klass: Frame

        frames = new Frames(null, Frame)
    ]
    @
]

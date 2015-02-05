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

            @setProperties intr

            @errorText = no
            @detailedErrorText = no
            @hasErrors = no
            @isLoading = yes
            @isTerminating = no
            @response  = null
            @templateUrl = intr.templateUrl
            timer = Timer.start()
            @startTime = timer.started()
            intrPromise = intrFn(query, $q.defer())
            @terminate = =>
              @isTerminating = yes
              intrPromise?.transaction?.rollback()?.then( =>
                @isTerminating = no
              )

            $q.when(intrPromise).then(
              (result) =>
                @isLoading = no
                @response = result
                @runTime = timer.stop().time()
              ,
              (result = {}) =>
                @isLoading = no
                @hasErrors = yes
                @response = null
                result = result[0] if Array.isArray result
                result = result.data?.errors[0] || result.errors?[0] || result
                @errorText = result.message or "Unknown error"
                @detailedErrorText = " " # ABKTODO consider a friendly message here
                if result.code?
                  @errorText = result.code
                  @detailedErrorText = result.message
                @runTime = timer.stop().time()

            )
            @
          setProperties: (intr) ->
            # FIXME: Make exportable a setting in commandInterperters.
            @exportable     = @type in ['cypher', 'http']
            @fullscreenable = if intr.fullscreenable is yes or typeof intr.fullscreenable is 'undefined' or intr.fullscreenable is null then yes else @fullscreenable


        class Frames extends Collection
          create: (data = {})  ->
            return unless data.input
            intr = @interpreterFor(data.input)
            return undefined unless intr
            if intr.templateUrl
              frame = new Frame(data)
            else
              rv = $injector.invoke(intr.exec)(data.input)

            if frame
              # Make sure we don't create more frames than allowed
              @add(frame.exec())
              @close(@first()) until @length <= Settings.maxFrames
            frame or rv

          close: (frame) ->
            @remove(frame)
            frame.terminate()

          createOne: (data = {}) ->
            last = @last()
            return if last?.input == data.input
            @create data

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

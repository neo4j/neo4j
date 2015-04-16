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
.provider 'Frame', [
  ->
    self = @
    @interpreters = []

    @$get = [
      '$injector'
      '$q'
      'Collection'
      'Settings'
      'Utils'
      ($injector, $q, Collection, Settings, Utils) ->
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
            # Find first matching input interpreter
            intr = frames.interpreterFor(query)
            return unless intr
            @type = intr.type
            intrFn = $injector.invoke(intr.exec)

            @setProperties intr

            @errorText = no
            @detailedErrorText = no
            @hasErrors = no
            @isLoading = yes unless @startTime
            @isTerminating = no
            @closeAttempts = 0
            @response  = null
            @templateUrl = intr.templateUrl
            @startTime = (new Date).getTime() unless @startTime
            @pinTime = 0
            intrPromise = intrFn(query, $q.defer())
            @terminate = =>
              @resetError()
              q = $q.defer()
              if not intrPromise or not intrPromise.transaction
                q.resolve({})
                return q.promise
              else
                intrPromise.reject 'cancel main request'
              @isTerminating = yes
              intrPromise.transaction.rollback().then(
                (r) =>
                  @isTerminating = no
                  q.resolve r
                ,
                (r) =>
                  @isTerminating = no
                  q.reject r
              )
              return q.promise

            $q.when(intrPromise).then(
              (result) =>
                @isLoading = no
                @response = result
              ,
              (result = {}) =>
                @isLoading = no
                @response = null
                @setError result
            )
            @
          setProperties: (intr) ->
            # FIXME: Make exportable a setting in commandInterperters.
            @exportable     = @type in ['cypher', 'http']
            @fullscreenable = if intr.fullscreenable is yes or typeof intr.fullscreenable is 'undefined' or intr.fullscreenable is null then yes else @fullscreenable

          setErrorMessages: (result = {}) =>
            #The "result" from a Cypher syntax error will have the correct format
            #which is {errors:[{code:string, message:string}]}.
            #We have to convert the other types.

            #This happens when the connection to the server is lost.
            result = {errors:[{code: 'Error', message: result}]} if typeof result is "string"

            #This happens for auth errors. We need to grab the Neo4j error code and message.
            result = result.data if result.status in [401, 429, 422, 404] and result.data.errors

            #This happens for HTTP status error codes
            result = {errors:[{code: "HTTP Status: #{result.status}", message: "HTTP Status: #{result.status} - #{result.statusText}"}]} if result.status

            errors = result.errors[0]
            @errorText = errors.code
            @detailedErrorText = errors.message

          resetError: =>
            @errorText = @detailedErrorText = ''
            @hasErrors = no
          addErrorText: (error) =>
            @detailedErrorText += error
            @hasErrors = yes
          setError: (response) =>
            @setErrorMessages response
            @hasErrors = yes
          getDetailedErrorText: =>
            @detailedErrorText

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
              @close(@first()) if @length > Settings.maxFrames
            frame or rv

          close: (frame) ->
            pr = frame.terminate()
            pr.then(
              =>
                @remove frame
              ,
              (r) =>
                return @remove frame unless frame.closeAttempts < 1
                frame.setError r
                frame.closeAttempts++
            )

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

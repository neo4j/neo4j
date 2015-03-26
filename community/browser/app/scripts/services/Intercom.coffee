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
  .service 'Intercom', [
    '$log'
    '$window'
    '$timeout'
    ($log, $window, $timeout) ->

      class IntercomService

        constructor: ->
          @_Intercom = $window.Intercom
          @booted = false

        do: (command, params...) ->
          # console.log("do", command, params)
          that = @
          args = arguments
          $timeout(-> $window.Intercom.apply(that, args))

        user: (userID, userData) ->
          intercomSettings = {
            app_id: 'lq70afwx'
            user_id: userID
          }
          angular.extend(intercomSettings, userData)
          if not @booted
            @do('boot', intercomSettings)
            @_Intercom('hide')
            @_Intercom('onShow', () =>
              @isShowing = true
              )
            @_Intercom('onHide', () =>
              @isShowing = false
              # TODO intercom should only run on demand. figure out how/when to shutdown&re-boot
              # @_Intercom('shutdown')
              # @booted = false
              )
            @booted = true

        toggle: () ->
          if @isShowing
            @do('hide')
          else
            @isShowing = true
            @do('show')

        newMessage: (message) ->
          @do('showNewMessage', message)

        update: (userData) ->
          @do('update', userData)

        event: (eventName, eventData) ->
          @do('trackEvent', eventName, eventData)


      new IntercomService()
    ]

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
.service 'ConnectionStatusService', [
  '$rootScope'
  'AuthDataService'
  ($rootScope, AuthDataService) ->

    updatePersistentAuthData = (data) ->
      return AuthDataService.clearAuthData() unless data
      AuthDataService.setAuthData data

    @connected_user = ''
    @authorization_required = true
    @is_connected = false

    @setConnectionAuthData = (username, password) ->
      @setConnectedUser username
      updatePersistentAuthData "#{username}:#{password}"
    @connectionAuthData = ->
      AuthDataService.getAuthData()
    @plainConnectionAuthData = ->
      data = AuthDataService.getPlainAuthData()
      if data then data.split(':') else ['','']
    @clearConnectionAuthData = ->
      @setConnectedUser ''
      @setAuthorizationRequired true
      @setConnected false
      updatePersistentAuthData false

    @setConnectedUser = (username) ->
      @connected_user = username
    @connectedAsUser = ->
      @connected_user

    @setAuthorizationRequired = (authorization_required) ->
      @authorization_required = authorization_required
    @authorizationRequired = ->
      return @authorization_required

    @setConnected = (is_connected) ->
      if @is_connected != is_connected
        $rootScope.$emit 'auth:status_updated'
      @is_connected = is_connected
    @isConnected = ->
      return @is_connected

    @getConnectionStatusSummary = ->
      {user: @connectedAsUser(), authorization_required: @authorizationRequired(), is_connected: @isConnected()}
    #Load user from AuthDataService
    @setConnectedUser @plainConnectionAuthData()[0]

    @
]

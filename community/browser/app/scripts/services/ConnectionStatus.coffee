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
  'Settings'
  '$timeout'
  ($rootScope, AuthDataService, Settings, $timeout) ->

    @updatePersistentAuthData = (data) ->
      return AuthDataService.clearAuthData() unless data
      AuthDataService.setAuthData data

    @unpersistCredentials = ->
      AuthDataService.clearPersistentAuthData()

    @updateStoreCredentials = (storeCredentials)->
      if not $rootScope.neo4j?.enterpriseEdition
        storeCredentials = yes
      else if storeCredentials is yes
        storeCredentials = [no, 'false', 'no'].indexOf(Settings.storeCredentials) < 0 ? yes : no

      @unpersistCredentials() unless storeCredentials
      AuthDataService.setStoreCredentials storeCredentials

    @updateCredentialTimeout = (credentialTimeout) ->
      if not $rootScope.neo4j?.enterpriseEdition then credentialTimeout = 0
      AuthDataService.setCredentialTimeout credentialTimeout

    @connected_user = ''
    @authorization_required = yes
    @is_connected = no
    @session_start_time = new Date()
    @session_countdown = null
    @waiting_policies = no

    @setConnectionAuthData = (username, password) ->
      @setConnectedUser username
      @updatePersistentAuthData "#{username}:#{password}"
    @connectionAuthData = ->
      AuthDataService.getAuthData()
    @plainConnectionAuthData = ->
      data = AuthDataService.getPlainAuthData()
      if data then data.split(':') else ['','']
    @clearConnectionAuthData = ->
      @setConnectedUser ''
      @updatePersistentAuthData false
      AuthDataService.clearPolicies()

    @setConnectedUser = (username) ->
      @connected_user = username
    @connectedAsUser = ->
      @connected_user

    @setAuthorizationRequired = (authorization_required) ->
      @authorization_required = authorization_required
    @authorizationRequired = ->
      return @authorization_required

    @setConnected = (is_connected) ->
      old_connection = @is_connected
      @is_connected = is_connected
      if old_connection != is_connected
        $rootScope.$emit 'auth:status_updated', is_connected
      $rootScope.$emit 'auth:disconnected' unless is_connected

    @isConnected = ->
      return @is_connected

    @setAuthPolicies = (policies) ->
      if not $rootScope.neo4j or not $rootScope.neo4j.version
        return @waiting_policies = policies
      if not policies.storeCredentials
        @updateStoreCredentials no
      else if policies.storeCredentials
        @updateStoreCredentials yes
        AuthDataService.persistCachedAuthData()
      if @getCredentialTimeout() isnt policies.credentialTimeout
        @updateCredentialTimeout policies.credentialTimeout
        @restartSessionCountdown()
      @waiting_policies = no

    @getStoreCredentials = ->
      AuthDataService.getPolicies().storeCredentials

    @getCredentialTimeout = ->
      AuthDataService.getPolicies().credentialTimeout

    @setSessionStartTimer = (start_date) ->
      @session_start_time = start_date
      @startSessionCountdown()

    @startSessionCountdown = ->
      if not @isConnected()
        @clearSessionCountdown()
        return
      if not AuthDataService.getPolicies().credentialTimeout
        @clearSessionCountdown()
        return
      that = @
      @session_start_time = new Date()
      ttl = (new Date()).getTime()/1000 + AuthDataService.getPolicies().credentialTimeout - @session_start_time.getTime()/1000
      @session_countdown = $timeout(->
        that.clearConnectionAuthData()
        that.setConnected no
      ,
        ttl*1000
      )

    @clearSessionCountdown = ->
      if @session_countdown then $timeout.cancel @session_countdown

    @restartSessionCountdown = ->
      @clearSessionCountdown()
      @startSessionCountdown()

    @getConnectionAge = ->
      Math.ceil(((new Date()).getTime() - @session_start_time.getTime())/1000)

    @getConnectionStatusSummary = ->
      {
        user: @connectedAsUser(),
        authorization_required: @authorizationRequired(),
        is_connected: @isConnected(),
        store_credentials: @getStoreCredentials(),
        credential_timeout: @getCredentialTimeout(),
        connection_age: @getConnectionAge()
      }

    $rootScope.$on('settings:saved', ->
      that.setAuthPolicies({storeCredentials: Settings.storeCredentials, credentialTimeout: AuthDataService.getPolicies().credentialTimeout})
      if AuthDataService.getPolicies().storeCredentials is no
        that.unpersistCredentials()
      else
        AuthDataService.persistCachedAuthData()
    )

    $rootScope.$on 'db:updated:edition', (e, edition) ->
      AuthDataService.clearPolicies()
      if that.waiting_policies then that.setAuthPolicies that.waiting_policies

    #Load user from AuthDataService
    @setConnectedUser @plainConnectionAuthData()[0]
    that = @
    @
]

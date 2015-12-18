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
.service 'AuthService', [
  'ConnectionStatusService'
  'Server'
  'Settings'
  '$q'
  (ConnectionStatusService, Server, Settings, $q) ->

    setConnectionAuthData = (username, password) ->
      ConnectionStatusService.setConnectionAuthData username, password

    clearConnectionAuthData = ->
      ConnectionStatusService.clearConnectionAuthData()

    class AuthService
      constructor: ->

      authenticate: (username, password) =>
        that = @
        @current_password = password
        setConnectionAuthData username, password

        promise = @makeRequest()
        promise.then(
          (r) ->
            ConnectionStatusService.setConnected yes
            r
          ,
          (r) ->
            that.forget() unless r.status == 403 #Forbidden
            r
        )
        promise

      authorizationRequired: ->
        #Make call without auth headers
        q = $q.defer()
        p = @makeRequest(skip_auth_header = yes)
        p.then(
          (r) ->
            ##Success, no auth required
            clearConnectionAuthData()
            ConnectionStatusService.setAuthorizationRequired no
            q.resolve r
          ,
          (r) ->
            ConnectionStatusService.setAuthorizationRequired yes
            q.reject r
        )
        q.promise

      hasValidAuthorization: ->
        that = @
        q = $q.defer()
        req = @authorizationRequired()
        req.then(
          (r) ->
            ConnectionStatusService.setConnected yes
            q.resolve r
          ,
          (r) ->
            that.isConnected().then(
              (r) ->
                q.resolve r
              ,
              (r) ->
                q.reject r
            )
        )
        q.promise

      isConnected: ->
        q = $q.defer()
        p = @makeRequest()
        p.then(
          (rr) ->
            ConnectionStatusService.setConnected yes
            q.resolve rr
        ,
          (rr) ->
            if rr.status is 401
              clearConnectionAuthData()
            q.reject rr
        )
        q.promise

      makeRequest: (skip_auth_header = no) ->
        opts = if skip_auth_header then {skipAuthHeader: skip_auth_header} else {}
        p = Server.get("#{Settings.endpoint.rest}/", opts)

      forget: =>
        if ConnectionStatusService.connectedAsUser()
          clearConnectionAuthData()
        @hasValidAuthorization()

      setNewPassword: (old_passwd, new_passwd) ->
        q = $q.defer()
        that = @
        setConnectionAuthData ConnectionStatusService.connectedAsUser(), old_passwd
        Server.post("#{Settings.endpoint.authUser}/#{ConnectionStatusService.connectedAsUser()}/password"
          , {password: new_passwd})
        .then(
          (r) ->
            setConnectionAuthData ConnectionStatusService.connectedAsUser(), new_passwd
            q.resolve r
          ,
          (r) ->
            that.forget() if r.status is 401
            q.reject r
        )
        q.promise

      getCurrentUser: ->
        ConnectionStatusService.connectedAsUser()

    new AuthService()
]

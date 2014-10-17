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
.service 'AuthService', [
  'AuthDataService',
  'Server'
  'Settings'
  '$rootScope'
  (AuthDataService, Server, Settings, $rootScope) ->

    updatePersistentAuthToken = (response) ->
      return AuthDataService.clearAuthData() unless response.authorization_token?
      AuthDataService.setAuthData ":#{response.authorization_token}"

    class AuthService
      @is_authenticated = false
      @current_user = false

      authenticate: (username, password) =>
        that = @
        promise = Server.post(Settings.endpoint.auth, {user: username, password: password})
        promise.then(
          (r) ->
            response = r.data
            updatePersistentAuthToken response
            that.is_authenticated = true
            that.current_user ?= response.user
            if response.password_change_requested is true
              $rootScope.$emit 'auth:password_change_requested', response.user
            r
          ,
          (r) ->
            that.forget()
            r
        )
        promise

      forget: =>
        updatePersistentAuthToken false
        @is_authenticated = false
        @current_user = false

      getAuthInfo: ->
        promise = Server.get(Settings.endpoint.auth)
        promise

      setNewPassword: (old_passwd, new_passwd) ->
        promise = Server.put("#{Settings.endpoint.authUser}/#{@current_user}/password"
          , {password: old_passwd, new_password: new_passwd})
        promise

      setNewAuthToken: (passwd, new_authorization_token) ->
        promise = Server.put("#{Settings.endpoint.authUser}/#{@current_user}/authorization_token"
          , {password: passwd, new_authorization_token: new_authorization_token})
        .then( ->
          updatePersistentAuthToken {authorization_token: new_authorization_token}
        )
        promise

      generateNewAuthToken: (passwd) ->
        promise = Server.post("#{Settings.endpoint.authUser}/#{@current_user}/authorization_token"
          , {password: passwd})
        promise.then((r)->
          response = r.data
          updatePersistentAuthToken response
        )
        promise
      
    new AuthService()
]

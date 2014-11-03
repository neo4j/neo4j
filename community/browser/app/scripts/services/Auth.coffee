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
      constructor: ->
        @current_user = false

      authenticate: (username, password) =>
        that = @
        @current_password = password
        promise = Server.post(Settings.endpoint.auth, {username: username, password: password})
        promise.then(
          (r) ->
            response = r.data
            that.current_user = response
            
            return r if response.password_change_required is true
            updatePersistentAuthToken response
            that.setAuthenticatedStatus true
            r
          ,
          (r) ->
            that.forget()
            r
        )
        promise

      hasValidAuthorization: =>
        that = @
        p = Server.get(Settings.endpoint.auth)
        p.success(
          (r) -> 
            that.setAuthenticatedStatus true
            that.current_user = r
            r
        ).error(
          (r) ->
            that.setAuthenticatedStatus false
            r
        )

      forget: =>
        updatePersistentAuthToken false
        if @current_user 
          @current_user = false
        @hasValidAuthorization()

      getAuthInfo: ->
        that = @
        Server.get(Settings.endpoint.auth).then( (r)->
          that.current_user = r.data
        )

      setNewPassword: (old_passwd, new_passwd) ->
        that = @
        promise = Server.post("#{Settings.endpoint.authUser}/#{@current_user.username}/password"
          , {password: old_passwd, new_password: new_passwd})
        .then(
          (r) -> 
            updatePersistentAuthToken r.data
            that.setAuthenticatedStatus true
        )
        promise

      setNewAuthToken: (passwd, new_authorization_token) ->
        promise = Server.post("#{Settings.endpoint.authUser}/#{@current_user.username}/authorization_token"
          , {password: passwd, new_authorization_token: new_authorization_token})
        .then( ->
          updatePersistentAuthToken {authorization_token: new_authorization_token}
        )
        promise

      generateNewAuthToken: (passwd) ->
        promise = Server.post("#{Settings.endpoint.authUser}/#{@current_user.username}/authorization_token"
          , {password: passwd})
        promise.then((r)->
          response = r.data
          updatePersistentAuthToken response
        )
        promise

      isAuthenticated: -> AuthDataService.isAuthenticated()

      setAuthenticatedStatus: (is_authenticated) =>
        AuthDataService.setAuthenticated is_authenticated
        $rootScope.$emit 'auth:status_updated'
      
    new AuthService()
]

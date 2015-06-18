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

'use strict'

angular.module('neo4jApp.controllers')
  .controller 'AuthCtrl', [
    '$scope'
    'AuthService'
    'ConnectionStatusService'
    'Frame'
    'Settings'
    '$timeout'
    ($scope, AuthService, ConnectionStatusService, Frame, Settings, $timeout) ->
      $scope.username = 'neo4j'
      $scope.password = ''
      $scope.current_password = ''
      $scope.connection_summary = ConnectionStatusService.getConnectionStatusSummary()
      $scope.static_user = $scope.connection_summary.user
      $scope.static_is_authenticated = $scope.connection_summary.is_connected
      $scope.policy_message = ''

      setPolicyMessage = ->
        return unless $scope.static_is_authenticated
        _connection_summary = ConnectionStatusService.getConnectionStatusSummary()
        if _connection_summary.credential_timeout is null
          $timeout(->
            setPolicyMessage()
          , 1000)
          return
        msg = ""
        if _connection_summary.store_credentials
          msg += "Connection credentials are stored in your web browser"
        else
          msg += "Connection credentials are not stored in your web browser"
        if _connection_summary.credential_timeout > 0
          msg += " and your credential timeout when idle is #{_connection_summary.credential_timeout} seconds."
        else
          msg += "."
        $scope.$evalAsync(->
          $scope.policy_message = msg
        )

      $scope.authenticate = ->
        $scope.frame.resetError()
        if not $scope.password.length
          $scope.frame.addErrorText 'You have to enter a password. '
        if not $scope.username.length
          $scope.frame.addErrorText 'You have to enter a username. '
        return if $scope.frame.getDetailedErrorText().length


        AuthService.authenticate($scope.username, $scope.password).then(
          (r) ->
            $scope.frame.resetError()
            $scope.connection_summary = ConnectionStatusService.getConnectionStatusSummary()
            $scope.static_user = $scope.connection_summary.user
            $scope.static_is_authenticated = $scope.connection_summary.is_connected
            setPolicyMessage()
            Frame.create({input:"#{Settings.initCmd}"})
            $scope.focusEditor()
          ,
          (r) ->
            if r.status is 403 and r.data.password_change?.length
              $scope.current_password = $scope.password
              return $scope.password_change_required = true
            $scope.frame.setError r
        )

      $scope.defaultPasswordChanged = ->
        AuthService.hasValidAuthorization().then(->
          $scope.password_change_required = false
          $scope.static_user = ConnectionStatusService.connectedAsUser()
          $scope.static_is_authenticated = ConnectionStatusService.isConnected()
        )

      setPolicyMessage()
  ]

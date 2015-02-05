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

'use strict'

angular.module('neo4jApp.controllers')
  .controller 'AuthCtrl', [
    '$scope'
    'AuthService'
    'ConnectionStatusService'
    'Frame'
    'Settings'
    ($scope, AuthService, ConnectionStatusService, Frame, Settings) ->
      $scope.username = 'neo4j'
      $scope.password = ''
      $scope.error_text = ''
      $scope.current_password = ''
      $scope.connection_summary = ConnectionStatusService.getConnectionStatusSummary()
      $scope.static_user = $scope.connection_summary.user
      $scope.static_is_authenticated = $scope.connection_summary.is_connected

      $scope.authenticate = ->
        $scope.error_text = ''
        if not $scope.password.length
          $scope.error_text += 'You have to enter a password. '
        if not $scope.username.length
          $scope.error_text += 'You have to enter a username. '
        return if $scope.error_text.length

        AuthService.authenticate($scope.username, $scope.password).then(
          (r) ->
              $scope.error_text = ''
              $scope.connection_summary = ConnectionStatusService.getConnectionStatusSummary()
              $scope.static_user = $scope.connection_summary.user
              $scope.static_is_authenticated = $scope.connection_summary.is_connected

              Frame.create({input:"#{Settings.cmdchar}play welcome"})
              $scope.focusEditor()
          ,
          (r) ->
            if r.status is 403 and r.data.errors[0].password_change.length
              $scope.current_password = $scope.password
              return $scope.password_change_required = true
            $scope.error_text = r.data.errors[0].message or "Server response code: #{r.status}"
        )

      $scope.defaultPasswordChanged = ->
        AuthService.hasValidAuthorization().then(->
          $scope.password_change_required = false
          $scope.static_user = ConnectionStatusService.connectedAsUser()
          $scope.static_is_authenticated = ConnectionStatusService.isConnected()
        )
  ]

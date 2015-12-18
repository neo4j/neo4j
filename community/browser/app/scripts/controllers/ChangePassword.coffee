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
  .controller 'ChangePasswordCtrl', [
    '$scope'
    'AuthService'
    'ConnectionStatusService'
    'Frame'
    'Settings'
    ($scope, AuthService, ConnectionStatusService, Frame, Settings) ->
      $scope.new_password = ''
      $scope.new_password2 = ''
      $scope.current_password = ''
      $scope.password_changed = false
      $scope.$parent.frame.resetError()
      $scope.static_user = ConnectionStatusService.connectedAsUser()
      $scope.static_is_authenticated = ConnectionStatusService.isConnected()

      $scope.showCurrentPasswordField = ->
        !$scope.$parent.password_change_required

      $scope.setNewPassword = ->
        is_authenticated = ConnectionStatusService.isConnected()
        $scope.$parent.frame.resetError()
        $scope.current_password = $scope.$parent.current_password if $scope.$parent.password_change_required
        $scope.static_user = ConnectionStatusService.connectedAsUser()

        if not $scope.current_password.length
          $scope.$parent.frame.addErrorText 'You have to enter your current password. '
        if not $scope.new_password.length
          $scope.$parent.frame.addErrorText 'You have to enter a new password. '
        if $scope.new_password != $scope.new_password2
          $scope.$parent.frame.addErrorText 'The new passwords mismatch, try again. '
        return if $scope.$parent.frame.getDetailedErrorText().length

        AuthService.setNewPassword($scope.current_password, $scope.new_password).then(
          ->

            #New user who just changed the default password.
            if not is_authenticated
              $scope.$parent.defaultPasswordChanged()
              Frame.create({input:"#{Settings.initCmd}"})

            $scope.password_changed = true
            $scope.$parent.frame.resetError()
            $scope.focusEditor()
          ,
          (r) ->
            $scope.$parent.frame.setError r
        )
  ]

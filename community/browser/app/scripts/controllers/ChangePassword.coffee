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
  .controller 'ChangePasswordCtrl', [
    '$scope'
    'AuthService'
    'Frame'
    'Settings'
    ($scope, AuthService, Frame, Settings) ->
      $scope.new_password = ''
      $scope.new_password2 = ''
      $scope.current_password = ''
      $scope.password_changed = false
      $scope.$parent.error_text = ''
      $scope.auth_service = AuthService

      $scope.showCurrentPasswordField = ->
        !$scope.$parent.password_change_required

      $scope.setNewPassword = ->
        is_authenticated = AuthService.isAuthenticated()
        $scope.$parent.error_text = ''
        $scope.current_password = $scope.$parent.current_password if $scope.$parent.password_change_required

        if not $scope.current_password.length
          $scope.$parent.error_text += 'You have to enter your current password. '
        if not $scope.new_password.length
          $scope.$parent.error_text += 'You have to enter a new password. '
        if $scope.new_password != $scope.new_password2
          $scope.$parent.error_text += 'The new passwords mismatch, try again. '
        return if $scope.$parent.error_text.length

        AuthService.setNewPassword($scope.current_password, $scope.new_password).then(
          -> 
          
            #New user who just changed the default password.
            if not is_authenticated
              Frame.create({input:"#{Settings.cmdchar}play welcome"})

            $scope.password_changed = true
            $scope.$parent.error_text = ''
            $scope.focusEditor()
          ,
          (r) ->
            $scope.$parent.error_text = r.data.errors[0].message or "Server response code: #{r.status}"
        )
  ]

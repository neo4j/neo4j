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
    ($scope, AuthService, Frame) ->
      $scope.new_password = ''
      $scope.new_password2 = ''
      $scope.current_password = AuthService.current_password
      $scope.password_changed = false

      $scope.setNewPassword = ->
        $scope.mismatch_error = false
        $scope.empty_error = false
        $scope.current_empty_error = false
        errors = 0

        if not $scope.new_password.length
          $scope.empty_error = true
          errors++
        if not $scope.current_password.length
          $scope.current_empty_error = true
          errors++
        if $scope.new_password != $scope.new_password2
          $scope.mismatch_error = true
          errors++
        return if errors > 0

        AuthService.setNewPassword($scope.current_password, $scope.new_password).then(
          -> 
            Frame.create({input:":play"})
            $scope.password_changed = true
        )
  ]

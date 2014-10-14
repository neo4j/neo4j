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
  .controller 'GenerateTokenCtrl', [
    '$scope'
    'AuthService'
    'Frame'
    'Settings'
    ($scope, AuthService, Frame, Settings) ->
      $scope.current_password = ''
      $scope.token_changed = false
      $scope.error_text = ''

      $scope.generateToken = ->
        $scope.error_text = ''

        if not $scope.current_password.length
          $scope.error_text = 'You have to enter your current password.'
        return if $scope.error_text.length

        AuthService.generateNewAuthToken($scope.current_password).then(
          -> 
            Frame.create({input:"#{Settings.cmdchar}server status"})
            $scope.token_changed = true
            $scope.error_text = ''
          ,
          (r) ->
            $scope.error_text = r.data.errors[0].message or "Server response code: #{r.status}"
        )

      $scope.setToken = ->
        $scope.error_text = ''

        if not $scope.current_password.length
          $scope.error_text = 'You have to enter your current password. '
        if not $scope.new_token.length
          $scope.error_text = 'You have to enter a new authentication token.'
        return if $scope.error_text.length

        AuthService.setNewAuthToken($scope.current_password, $scope.new_token).then(
          -> 
            Frame.create({input:"#{Settings.cmdchar}server status"})
            $scope.token_changed = true
            $scope.error_text = ''
          ,
          (r) ->
            $scope.error_text = r.data.errors[0].message or "Server response code: #{r.status}"
        )
  ]

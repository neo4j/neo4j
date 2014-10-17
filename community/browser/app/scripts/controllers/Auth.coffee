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
    'Frame'
    ($scope, AuthService, Frame) ->
      $scope.username = 'neo4j'
      $scope.password = ''
      $scope.authenticated = no
      $scope.invalid_auth_error = false

      $scope.authenticate = ->
        $scope.empty_error = false
        $scope.current_empty_error = false
        errors = 0

        if not $scope.password.length
          $scope.empty_error = true
          errors++
        if not $scope.username.length
          $scope.current_empty_error = true
          errors++
        return if errors > 0

        AuthService.authenticate($scope.username, $scope.password).then(
          (r) -> 
            $scope.authenticated = yes
            Frame.create({input:':play'})
          ,
          (r) -> 
            $scope.invalid_auth_error = true
        )
  ]

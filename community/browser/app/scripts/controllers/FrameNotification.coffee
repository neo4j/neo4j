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
.controller 'FrameNotificationCtrl', ['$scope', '$timeout', ($scope, $timeout) ->

  $scope.show = no
  $scope.message = ''
  $scope.type = 'default'
  closeTimeout = null

  show = (type, message, ttl = 2500) ->
    if $scope.show
      setTimeout(->
        show(type, message)
      , ttl/5)
      return
    $scope.type = type
    $scope.message = message
    $scope.show = yes

    closeTimeout = $timeout(->
      $scope.close()
    ,
      ttl
    )

  $scope.close = ->
    if closeTimeout
      $timeout.cancel closeTimeout
    $scope.show = no
    $scope.message = ''

  $scope.$on 'frame.notif.max_neighbour_limit', (event, result) ->
    msg = "Rendering was limited to #{result.neighbourDisplayedSize} of the node's total #{result.neighbourSize} connections "
    msg += "due to browser config maxNeighbours."
    show 'default', msg

]

###!
Copyright (c) 2002-2017 "Neo Technology,"
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
.controller 'FrameNotificationCtrl', ['$scope', '$timeout', 'Editor', 'Settings', ($scope, $timeout, Editor, Settings) ->

  $scope.notifications = []
  $$id = 0

  addNotification = (type = 'default', message, fn, ttl = 0) ->
    current_id = ++$$id
    obj =
      type: type
      message: message
      fn: fn
      '$$id': current_id
      '$$is_closing': no
    $scope.notifications.push obj
    $$timeout = $timeout( ->
      $scope.close obj
    ,
      ttl
    )
    obj['$$timeout'] = $$timeout
    obj


  $scope.close = (obj) ->
    if obj['$$timeout']
      $timeout.cancel obj['$$timeout']
    $timeout(->
      $scope.notifications = $scope.notifications.filter((item) ->
        item['$$id'] isnt obj['$$id']
      )
    ,
      700
    )
    obj['$$is_closing'] = yes


  $scope.$on 'frame.notif.max_neighbour_limit', (event, result) ->
    msg = "Rendering was limited to #{result.neighbourDisplayedSize} of the node's total #{result.neighbourSize} connections "
    msg += "due to browser config maxNeighbours."
    fn = ->
      Editor.setContent "#{Settings.cmdchar}config maxNeighbours: #{result.neighbourDisplayedSize}"
    addNotification 'default', msg, fn, 10000

]

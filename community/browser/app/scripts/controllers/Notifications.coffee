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
  .controller 'NotificationCtrl', [
    '$scope'
    '$sce'
    '$log'
    'Settings'
    'SettingsStore'
    ($scope, $sce, $log, Settings, SettingsStore) ->


      $scope.notifications = []

      $scope.hasNotifications = () ->
        ($scope.notifications.length > 0)

      $scope.defaultNotifications = [
          {
            setting:"shouldReportUdc"
            message:"Hello and thanks for downloading Neo4j! Help us make Neo4j even better by sharing <a href='http://neo4j.com/legal/neo4j-user-experience/'> non&#8209;sensitive data</a>. Would that be OK?"
            style:"warning"
            options:[
              {
                label: "Yes, I'm happy to help!"
                icon: "fa-smile-o"
                btn: "btn-good"
                value: true
              }
              {
                label: "Sorry no, but good luck"
                icon: "fa-frown-o"
                btn: "btn-neutral"
                value: false
              }
            ]
          }
        ]

      $scope.rememberThenDismiss = (notification, value) =>
        Settings[notification.setting] = value
        SettingsStore.save()
        $scope.notifications.shift()

      angular.forEach($scope.defaultNotifications, (notification) =>
        if not (Settings[notification.setting]?)
          $scope.notifications.push(notification)
      )

  ]

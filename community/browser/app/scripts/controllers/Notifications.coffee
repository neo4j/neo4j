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
            message:"Hello and thanks for downloading Neo4j! We'd like your help building a feedback graph to improve Neo4j features. Are you willing to <a href='http://neo4j.com/privacy-policy/'>share non&#8209;sensitive usage data</a>?"
            style:"warning"
            options:[
              {
                label: "Yes, sure!"
                icon: "fa-smile-o fa-good"
                value: true
              }
              {
                label: "Sorry, no"
                icon: "fa-frown-o fa-neutral"
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

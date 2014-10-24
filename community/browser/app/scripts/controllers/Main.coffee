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
  .config ($provide, $compileProvider, $filterProvider, $controllerProvider) ->
    $controllerProvider.register 'MainCtrl', [
      '$rootScope',
      '$window'
      'Server'
      'Frame'
      'AuthService'
      'Settings'
      'motdService'
      ($scope, $window, Server, Frame, AuthService, Settings, motdService) ->
        refresh = ->
          $scope.labels = Server.labels()
          $scope.relationships = Server.relationships()
          $scope.propertyKeys = Server.propertyKeys()
          $scope.server = Server.info()
          $scope.host = $window.location.host

        $scope.motd = motdService

        $scope.neo4j =
          license =
            type: "GPLv3"
            url: "http://www.gnu.org/licenses/gpl.html"
            edition: "Enterprise" # TODO: determine edition via REST
            hasData: Server.hasData()

        $scope.$on 'db:changed:labels', refresh

        $scope.today = Date.now()

        $scope.cmdchar = Settings.cmdchar

        $scope.goodBrowser = (navigator.appName != 'Microsoft Internet Explorer' && navigator.userAgent.indexOf('Trident') == -1)

        # gather info from jmx
        Server.jmx(
          [
            "org.neo4j:instance=kernel#0,name=Configuration"
            "org.neo4j:instance=kernel#0,name=Kernel"
            "org.neo4j:instance=kernel#0,name=Store file sizes"
          ]
        ).success((response) ->
          $scope.kernel = {}
          for r in response
            for a in r.attributes
              $scope.kernel[a.name] = a.value
        )

        $scope.$watch 'offline', (serverIsOffline) ->
          if not serverIsOffline
            refresh()
          else $scope.errorMessage = motdService.disconnected

        $scope.$watch 'unauthorized', (isUnauthorized) ->
          if not isUnauthorized
            refresh()
          else $scope.errorMessage = motdService.unauthorized

        # Authorization
        AuthService.hasValidAuthorization().then(
          -> Frame.create({input:":play"})
          ,
          -> 
            # Not authorized. Check if we can authenticate with neo4j:neo4j
            # This is simply a help to first-time users, so they don't need
            # to know the default password.
            AuthService.authenticate("neo4j", "neo4j").then(
              -> # This worked, nothing to do now
              ,
              -> 
                # User needs to log in
                Frame.create({input:":server connect"})
            )
        )

        # XXX: Temporary for now having to change all help files
        $scope.$watch 'server', (val) ->
          $scope.neo4j.version = val.neo4j_version
        , true

        refresh()
    ]

  .run([
    '$rootScope'
    'Editor'
    ($scope, Editor) ->
      # everything should be assembled
      # Editor.setContent(":play intro") 
      # Editor.execScript(":play intro")
  ])



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
  .config ($provide, $compileProvider, $filterProvider, $controllerProvider) ->
    $controllerProvider.register 'MainCtrl', [
      '$rootScope',
      '$window'
      'Server'
      'Frame'
      'AuthService'
      'ConnectionStatusService'
      'Settings'
      'motdService'
      'UsageDataCollectionService'
      'Utils'
      ($scope, $window, Server, Frame, AuthService, ConnectionStatusService, Settings, motdService, UDC, Utils) ->
        $scope.kernel = {}
        $scope.refresh = ->
          return '' if $scope.unauthorized || $scope.offline

          $scope.labels = Server.labels $scope.labels
          $scope.relationships = Server.relationships $scope.relationships
          $scope.propertyKeys = Server.propertyKeys $scope.propertyKeys
          $scope.server = Server.info $scope.server
          $scope.version = Server.version $scope.version
          $scope.host = $window.location.host

          # gather info from jmx
          Server.jmx(
            [
              "org.neo4j:instance=kernel#0,name=Configuration"
              "org.neo4j:instance=kernel#0,name=Kernel"
              "org.neo4j:instance=kernel#0,name=Store file sizes"
            ]).success((response) ->
              for r in response
                for a in r.attributes
                  $scope.kernel[a.name] = a.value
              UDC.set('store_id',   $scope.kernel['StoreId'])
              UDC.set('neo4j_version', $scope.server.neo4j_version)
              refreshPolicies $scope.kernel['dbms.browser.store_credentials'], $scope.kernel['dbms.browser.credential_timeout']
              allow_connections = [no, 'false', 'no'].indexOf($scope.kernel['dbms.security.allow_outgoing_browser_connections']) < 0 ? yes : no
              refreshAllowOutgoingConnections allow_connections
            ).error((r)-> $scope.kernel = {})

        refreshAllowOutgoingConnections = (allow_connections) ->
          return unless $scope.neo4j.config.allow_outgoing_browser_connections != allow_connections
          allow_connections = if $scope.neo4j.enterpriseEdition then allow_connections else yes
          mapServerConfig 'allow_outgoing_browser_connections', allow_connections
          if allow_connections 
            $scope.motd.refresh()
            UDC.loadUDC()
          else if not allow_connections
            UDC.unloadUDC()

        refreshPolicies = (storeCredentials = yes, credentialTimeout = 0) ->
          storeCredentials = [no, 'false', 'no'].indexOf(storeCredentials) < 0 ? yes : no
          credentialTimeout = Utils.parseTimeMillis(credentialTimeout) / 1000
          ConnectionStatusService.setAuthPolicies {storeCredentials, credentialTimeout}

        mapServerConfig = (key, val) ->
          return unless $scope.neo4j.config[key] != val
          $scope.neo4j.config[key] = val

        $scope.identity = angular.identity

        $scope.motd = motdService
        $scope.auth_service = AuthService

        $scope.neo4j =
          license =
            type: "GPLv3"
            url: "http://www.gnu.org/licenses/gpl.html"
            edition: 'community'
            enterpriseEdition: no
        $scope.neo4j.config = {}

        $scope.$on 'db:changed:labels', $scope.refresh

        $scope.today = Date.now()
        $scope.cmdchar = Settings.cmdchar

        #IE < 11 has MSIE in the user agent. IE >= 11 do not.
        $scope.goodBrowser = !/msie/.test(navigator.userAgent.toLowerCase())

        $scope.$watch 'offline', (serverIsOffline) ->
          if (serverIsOffline?)
            if not serverIsOffline
              UDC.ping("connect")
            else
              $scope.errorMessage = motdService.pickRandomlyFromChoiceName('disconnected')

        $scope.$on 'auth:status_updated', (e, is_connected) ->
          $scope.check()
          if is_connected
            ConnectionStatusService.setSessionStartTimer new Date()

        # Authorization
        AuthService.hasValidAuthorization().then(
          ->
            Frame.create({input:"#{Settings.initCmd}"})
          ,
          (r) ->
            if r.status is 404
              Frame.create({input:"#{Settings.initCmd}"})
            else
              $scope.$emit 'auth:disconnected'
        )

        $scope.$on 'auth:disconnected', ->
          Frame.createOne({input:"#{Settings.cmdchar}server connect"})

        $scope.$watch 'version', (val) ->
          return '' if not val

          $scope.neo4j.version = val.version
          $scope.neo4j.edition = val.edition
          $scope.neo4j.enterpriseEdition = val.edition is 'enterprise'
          $scope.$emit 'db:updated:edition', val.edition

          if val.version then $scope.motd.setCallToActionVersion(val.version)
        , true
    ]

  .run([
    '$rootScope'
    'Editor'
    ($scope, Editor) ->
      $scope.unauthorized = yes
      # everything should be assembled
      # Editor.setContent(":play intro")
      # Editor.execScript(":play intro")
  ])

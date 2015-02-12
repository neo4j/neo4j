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

describe 'Service: AuthService', () ->

  $scope = {}
  AuthService = {}
  Settings = {}
  httpBackend = {}
  AuthDataService = {}
  ConnectionStatusService = {}
  beforeEach ->
    module 'neo4jApp.services'

  beforeEach ->
    inject(($rootScope, _AuthService_, _Settings_, _AuthDataService_, _ConnectionStatusService_, $httpBackend) ->
      $scope = $rootScope
      AuthService = _AuthService_
      Settings = _Settings_
      AuthDataService = _AuthDataService_
      ConnectionStatusService = _ConnectionStatusService_
      httpBackend = $httpBackend
    )

  describe ' - Auth tests', ->

    it ' - Persist auth data in localstorage', ->
      httpBackend.when('GET', "#{Settings.endpoint.rest}/")
        .respond(->
          return [200, JSON.stringify({})]
        )

      data = {}
      AuthService.authenticate('test', 'test')
        .then( (response) ->
          data = response.data
        )
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()

      expect(AuthDataService.getAuthData()).toBe('dGVzdDp0ZXN0')
      expect(ConnectionStatusService.connectedAsUser()).toBe('test')


    it ' - Empty auth token in persistent storage on unsuccessful authentication', ->
      ConnectionStatusService.setConnectionAuthData('sample', 'error')
      httpBackend.when('GET', "#{Settings.endpoint.rest}/").respond(401, JSON.stringify({}))
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthDataService.getAuthData()).toBeFalsy()

    it ' - ConnectionStatusSummary should be correct when auth FAILED', ->
      ConnectionStatusService.setConnectionAuthData('sample', 'error')
      httpBackend.when('GET', "#{Settings.endpoint.rest}/").respond(401, JSON.stringify({}))
      AuthService.hasValidAuthorization()
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      status = ConnectionStatusService.getConnectionStatusSummary()
      expect(status.user).toBeFalsy()
      expect(status.is_connected).toBe false
      expect(status.authorization_required).toBe true

    it ' - ConnectionStatusSummary should be correct when auth is disabled on server', ->
      httpBackend.when('GET', "#{Settings.endpoint.rest}/").respond(200, JSON.stringify({}))
      AuthService.hasValidAuthorization()
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      status = ConnectionStatusService.getConnectionStatusSummary()
      expect(status.is_connected).toBe true
      expect(status.authorization_required).toBe false

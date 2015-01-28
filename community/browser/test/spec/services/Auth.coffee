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
      httpBackend.when('GET', "#{Settings.endpoint.rest}")
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
      httpBackend.when('GET', "#{Settings.endpoint.rest}").respond(401, JSON.stringify({}))
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthDataService.getAuthData()).toBeFalsy()

    it ' - ConnectionStatusSummary should be correct when auth FAILED', ->
      ConnectionStatusService.setConnectionAuthData('sample', 'error')
      httpBackend.when('GET', "#{Settings.endpoint.rest}").respond(401, JSON.stringify({}))
      AuthService.hasValidAuthorization()
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      status = ConnectionStatusService.getConnectionStatusSummary()
      expect(status.user).toBeFalsy()
      expect(status.is_connected).toBe false
      expect(status.authorization_required).toBe true

    it ' - ConnectionStatusSummary should be correct when auth is disabled on server', ->
      httpBackend.when('GET', "#{Settings.endpoint.rest}").respond(200, JSON.stringify({}))
      AuthService.hasValidAuthorization()
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      status = ConnectionStatusService.getConnectionStatusSummary()
      expect(status.is_connected).toBe true
      expect(status.authorization_required).toBe false

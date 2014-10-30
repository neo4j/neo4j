'use strict'

describe 'Service: AuthService', () ->

  $scope = {}
  AuthService = {}
  Settings = {}
  httpBackend = {}
  AuthDataService = {}
  SavedAuthServiceState = {}
  beforeEach ->
    module 'neo4jApp.services'

  beforeEach ->
    inject(($rootScope, _AuthService_, _Settings_, _AuthDataService_, $httpBackend) ->
      $scope = $rootScope
      AuthService = _AuthService_
      Settings = _Settings_
      AuthDataService = _AuthDataService_
      httpBackend = $httpBackend
    )

  describe ' - Basic Auth tests', ->

    it ' - Persist auth token on successful authentication, generation of new token and manual update of token', ->
      success_response = 
        user: 'test'
        authorization_token: 'longtoken'
      httpBackend.expect('POST', "#{Settings.endpoint.auth}")
        .respond(200, JSON.stringify(success_response));
      data = {}
      AuthService.authenticate('test', 'test')
        .then( (response) -> 
          data = response.data
        )
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(':' + data.authorization_token).toBe(AuthDataService.getAuthToken())
      
      token_reply = 
        authorization_token: 'newtoken'

      httpBackend.when('POST', "#{Settings.endpoint.authUser}/test/authorization_token"
        , {password: 'test'}
        , (headers) ->
          expect(headers.Authorization).toBe('Basic Omxvbmd0b2tlbg==')
          headers
      )
      .respond(200, JSON.stringify(token_reply))
      AuthService.generateNewAuthToken('test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(':newtoken').toBe(AuthDataService.getAuthToken())
      

      httpBackend.expect('POST', "#{Settings.endpoint.authUser}/test/authorization_token")
        .respond(200, JSON.stringify({}))
      AuthService.setNewAuthToken('test', 'manualnewtoken')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(':manualnewtoken').toBe(AuthDataService.getAuthToken())


    it ' - Flag is_authenticated is true on successful authentication', ->
      success_response = 
        user: 'neo4j'
        authorization_token: 'longtoken'
      httpBackend.expect('POST', "#{Settings.endpoint.auth}").respond(200, JSON.stringify(success_response));
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthService.isAuthenticated()).toBe true
      

    it ' - Empty auth token in persistent storage on unsuccessful authentication', ->
      AuthDataService.setAuthData('sample')
      httpBackend.expect('POST', "#{Settings.endpoint.auth}").respond(422, JSON.stringify({}));
      httpBackend.expect('GET', "#{Settings.endpoint.auth}").respond(401, JSON.stringify({}));
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthDataService.getAuthToken()).toBeFalsy()
      expect(AuthService.isAuthenticated()).toBe false

    it ' - isAuthenticated should always be true when auth is disabled on server', ->
      AuthDataService.setAuthData('sample')
      httpBackend.expect('GET', "#{Settings.endpoint.auth}").respond(200, JSON.stringify({}));
      AuthService.forget()
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthService.isAuthenticated()).toBe true

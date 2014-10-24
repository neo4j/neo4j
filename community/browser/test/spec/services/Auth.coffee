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

      httpBackend.expect('POST', "#{Settings.endpoint.authUser}/test/authorization_token")
        .respond(200, JSON.stringify(token_reply));
      AuthService.generateNewAuthToken('test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(':newtoken').toBe(AuthDataService.getAuthToken())

      httpBackend.expect('PUT', "#{Settings.endpoint.authUser}/test/authorization_token")
        .respond(200, JSON.stringify({}));
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
      expect(AuthService.is_authenticated).toBe true


    it ' - Emit event auth:password_change_requested on response.password_change_requested is true', ->
      success_response = 
        user: 'neo4j'
        password_change_requested: true
        authorization_token: 'longtoken'
      spyOn($scope, "$emit")
      httpBackend.expect('POST', "#{Settings.endpoint.auth}").respond(200, JSON.stringify(success_response));
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect($scope.$emit).toHaveBeenCalledWith('auth:password_change_requested', 'neo4j')


    it ' - Empty auth token in persistent storage on unsuccessful authentication', ->
      AuthDataService.setAuthData('sample')
      httpBackend.expect('POST', "#{Settings.endpoint.auth}").respond(422, JSON.stringify({}));
      AuthService.authenticate('test', 'test')
      $scope.$apply() if not $scope.$$phase
      httpBackend.flush()
      expect(AuthDataService.getAuthToken()).toBeFalsy()
      expect(AuthService.is_authenticated).toBe false

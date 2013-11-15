'use strict'

describe 'Controller: SidebarCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.controllers', 'neo4jApp.services'

  SidebarCtrl = {}
  scope = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()
    SidebarCtrl = $controller 'SidebarCtrl', {
      $scope: scope
    }

  describe 'createFolder:', ->

  describe 'removeDocument', ->

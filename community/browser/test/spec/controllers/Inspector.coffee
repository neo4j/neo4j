'use strict'

describe 'Controller: InspectorCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.controllers'

  InspectorCtrl = {}
  scope = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()
    InspectorCtrl = $controller 'InspectorCtrl', {
      $scope: scope
    }


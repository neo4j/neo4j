'use strict'

describe 'Controller: CypherResultCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.controllers'

  CypherResultCtrl = {}
  scope = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()
    CypherResultCtrl = $controller 'CypherResultCtrl', {
      $scope: scope
    }


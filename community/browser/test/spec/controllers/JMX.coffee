'use strict'

describe 'Controller: JMXCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.controllers'

  JMXCtrl = {}
  scope = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()
    JMXCtrl = $controller 'JMXCtrl', {
      $scope: scope
    }


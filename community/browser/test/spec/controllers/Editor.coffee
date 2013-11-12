'use strict'

describe 'Controller: EditorCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp'

  EditorCtrl = {}
  scope = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()
    EditorCtrl = $controller 'EditorCtrl', {
      $scope: scope
    }

'use strict'

describe 'Controller: StreamCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.services', 'neo4jApp.controllers'

  Frame = {}
  Folder = {}
  StreamCtrl = {}
  scope = {}
  timer = {}


  # Initialize the controller and a mock scope
  beforeEach ->
    module (FrameProvider) ->
      FrameProvider.interpreters.push
        type: ':help'
        matches: ':help'
        templateUrl: 'dummy.html'
        exec: ->
          (input) -> return true

      return

    inject ($controller, $rootScope, _Folder_, _Frame_, $timeout) ->
      scope = $rootScope.$new()
      Folder = _Folder_
      Frame = _Frame_
      timer = $timeout
      # Reset storage
      Folder.save([])

      # Instantiate
      StreamCtrl = $controller 'StreamCtrl', { $scope: scope }
      scope.$digest()

'use strict'

describe 'Controller: SidebarCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.controllers', 'neo4jApp.services'

  SidebarCtrl = {}
  scope = {}
  Document = {}

  # Initialize the controller and a mock scope
  beforeEach inject ($controller, $rootScope, _Document_) ->
    scope = $rootScope.$new()
    Document = _Document_
    SidebarCtrl = $controller 'SidebarCtrl', {
      $scope: scope
    }

  describe '#playDocument', ->
    it 'increases document play count', ->
      doc = Document.create()
      scope.playDocument(doc)
      expect(doc.metrics.total_runs).toBe 1

'use strict'

describe 'Service: Cypher', () ->
  [backend, node, rel] = [null, null, null]

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach inject ($httpBackend) ->
    backend = $httpBackend

  afterEach ->
    backend.verifyNoOutstandingRequest()

  # instantiate service
  Cypher = {}
  scope = {}
  beforeEach inject (_Cypher_, $rootScope) ->
    Cypher = _Cypher_
    scope = $rootScope.$new()

  describe "transaction:", ->
    it 'should execute statement and commit transaction', ->
      backend.expectPOST(/db\/data\/transaction\/commit/).respond()
      Cypher.transaction().commit('START n=node(*) RETURN n;')
      scope.$apply()
      backend.flush()
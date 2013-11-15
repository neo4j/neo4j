'use strict'

describe 'Service: GraphExplorer', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  GraphExplorer = {}
  beforeEach inject (_GraphExplorer_) ->
    GraphExplorer = _GraphExplorer_

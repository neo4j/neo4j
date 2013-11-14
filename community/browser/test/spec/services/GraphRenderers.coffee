'use strict'

describe 'Service: GraphRenderer', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  GraphRenderer = {}
  beforeEach inject (_GraphRenderer_) ->
    GraphRenderer = _GraphRenderer_

'use strict'

describe 'Service: Timer', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Timer = {}
  beforeEach inject (_Timer_) ->
    Timer = _Timer_



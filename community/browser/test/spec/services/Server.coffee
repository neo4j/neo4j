'use strict'

describe 'Service: Server', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Server = {}
  beforeEach inject (_Server_) ->
    Server = _Server_


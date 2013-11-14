'use strict'

describe 'Directive: clickToCode', ->

  # load the directive's module
  beforeEach module 'neo4jApp.directives'

  scope = {}

  beforeEach inject ($controller, $rootScope) ->
    scope = $rootScope.$new()

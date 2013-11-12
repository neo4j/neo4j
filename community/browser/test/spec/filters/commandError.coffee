'use strict'

describe 'Filter: commandError', () ->

  # load the filter's module
  beforeEach module 'neo4jApp.filters'

  # initialize a new instance of the filter before each test
  commandError = {}
  beforeEach inject ($filter) ->
    commandError = $filter 'commandError'

  it 'should handle empty input', ->
    fn = -> commandError()
    expect(fn).not.toThrow()
    expect(commandError()).toBe('Unrecognized')

  it 'should return "Unrecognized" for commands not beginning with ":"', ->
    expect(commandError('help')).toBe('Unrecognized')

  it 'should return "Not-a-command" for commands beginning with ":"', ->
    expect(commandError(':help')).toBe('Not-a-command')

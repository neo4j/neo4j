###!
Copyright (c) 2002-2015 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

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

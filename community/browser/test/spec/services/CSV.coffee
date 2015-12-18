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

describe 'Service: CSV', () ->
  serializer = null

  # load the service's module
  beforeEach module 'neo.csv'

  # instantiate service
  CSV = {}
  beforeEach inject (_CSV_) ->
    CSV = _CSV_

  describe 'Serializer:', ->
    beforeEach ->
      serializer = new CSV.Serializer
      serializer.columns(['col1', 'col2'])

    it 'should serlialize array to CSV', () ->
      expect(serializer.output()).toBe "col1,col2"

    it 'should escape qoute with double qoute in columns', ->
      serializer.columns(['col1', '"col2"'])
      expect(serializer.output()).toBe('col1,"""col2"""')

    it "should throw exception when row doesn't match columns", ->
      expect(serializer.append).toThrow()

    it "should insert a line break for each row", ->
      serializer.append(['data1', 'data2'])
      expect(serializer.output().split("\n").length).toEqual 2

    it "should escape delimiter characters in data", ->
      serializer.columns(['column, first', 'column, second'])
      expect(serializer.output()).toBe('"column, first","column, second"')

    it 'should both escape and qoute data', ->
      serializer.columns(['column, "first"', 'column, "second"'])
      expect(serializer.output()).toBe('"column, ""first""","column, ""second"""')

    it 'should not strip whitespace in data', ->
      serializer.columns([' column', ' column2 '])
      expect(serializer.output()).toBe(' column, column2 ')

    it 'should represent null values as null', ->
      serializer.columns([null, 'col'])
      expect(serializer.output()).toBe(',col')

    it 'should represent empty values as ""', ->
      serializer.columns(['', 'col'])
      expect(serializer.output()).toBe('"",col')

    it 'should represent boolean values as "true" and "false"', ->
      serializer.columns([true, false])
      expect(serializer.output()).toBe('true,false')

    it 'should represent Object values as JSON', ->
      serializer.columns([{name: 'John'}])
      expect(serializer.output()).toBe('"{""name"":""John""}"')

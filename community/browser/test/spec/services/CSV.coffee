'use strict'

describe 'Service: CSV', () ->
  serializer = null

  # load the service's module
  beforeEach module 'neo4jApp.services'

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

    it 'should represent null values as "null"', ->
      serializer.columns([null, 'col'])
      expect(serializer.output()).toBe('null,col')

    it 'should represent boolean values as "true" and "false"', ->
      serializer.columns([true, false])
      expect(serializer.output()).toBe('true,false')

    it 'should represent Object values as JSON', ->
      serializer.columns([{name: 'John'}])
      expect(serializer.output()).toBe('"{""name"":""John""}"')

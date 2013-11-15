'use strict'

describe 'Filter: autotitle', () ->

  # load the filter's module
  beforeEach module 'neo4jApp.filters'

  # initialize a new instance of the filter before each test
  autotitle = {}
  beforeEach inject ($filter) ->
    autotitle = $filter 'autotitle'

  it 'should return the raw code:"', () ->
    text = 'START n=node(0)\nMATCH n-[r:KNOWS*]-m\nRETURN n AS Neo,r,m'
    expect(autotitle text).toBe text

  it 'should return the first comment as title:"', () ->
    text = '//My script\nSTART n=node(0)\nMATCH n-[r:KNOWS*]-m\nRETURN n AS Neo,r,m'
    expect(autotitle text).toBe 'My script'

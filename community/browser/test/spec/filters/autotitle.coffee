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

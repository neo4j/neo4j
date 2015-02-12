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

describe 'Utils: firstWord', () ->



  # load the service's module
  beforeEach module 'neo4jApp.utils'

  # instantiate service
  Utils = {}
  beforeEach ->
    inject (_Utils_) ->
      Utils = _Utils_

  it 'should get first word in a multiword line', ->
    text = "multiple words here on one line"
    expect(Utils.firstWord text).toBe 'multiple'

  it 'should get first word in a multiline string', ->
    text = """
          cypher queries
          will often be more
          legible on multiple lines 
          than squashed onto a single line
          """
    expect(Utils.firstWord text).toBe 'cypher'

  it 'should get first word when it is alone in a multiline string', ->
    text = """
          alone
          on the first line but
          still extractable
          """
    expect(Utils.firstWord text).toBe 'alone'
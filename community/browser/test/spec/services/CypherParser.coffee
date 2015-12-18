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

describe 'Service: CypherParser', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  CypherParser = {}
  beforeEach inject (_CypherParser_) ->
    CypherParser = _CypherParser_

  it 'should find these queries to be periodic commits', ->
    shouldMatchPeriodicCommit = [
      "Using Periodic Commit"
      "Using Periodic Commit\n"
      " Using Periodic Commit 200"
      "//This is a comment\nUSING PERIODIC COMMIT"
    ]
    for query in shouldMatchPeriodicCommit
      expect(CypherParser.isPeriodicCommit(query)).toBe(true)

  it 'should find these queries NOT to be periodic commits', ->
    shouldNotMatchPeriodicCommit = [
          "MATCH (r:Person) WHERE "
          "//Comment\nMATCH (r:Person) WHERE "
          'MATCH (r:Person {title: "USING PERIODIC COMMIT"}) '
        ]
    for query in shouldNotMatchPeriodicCommit
      expect(CypherParser.isPeriodicCommit(query)).toBe(false)

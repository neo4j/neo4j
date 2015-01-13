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

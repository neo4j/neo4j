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

describe 'Service: Cypher', () ->
  [backend, node, rel] = [null, null, null]

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach inject ($httpBackend) ->
    backend = $httpBackend

  afterEach ->
    backend.verifyNoOutstandingRequest()

  # instantiate service
  Cypher = {}
  scope = {}
  beforeEach inject (_Cypher_, $rootScope) ->
    Cypher = _Cypher_
    scope = $rootScope.$new()

  describe "transaction:", ->
    it 'should execute statement and commit transaction', ->
      backend.expectPOST(/db\/data\/transaction\/commit/).respond()
      Cypher.transaction().commit('START n=node(*) RETURN n;')
      scope.$apply()
      backend.flush()
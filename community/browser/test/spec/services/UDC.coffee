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

describe 'Service: UDC', () ->
  UsageDataCollectionService = {}
  Settings = {}
  httpBackend = {}
  Cypher = {}
  scope = {}

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach ->
    inject((_UsageDataCollectionService_, _Settings_, $httpBackend, _Cypher_, $rootScope) ->
      UsageDataCollectionService = _UsageDataCollectionService_
      Settings = _Settings_
      httpBackend = $httpBackend
      Cypher = _Cypher_
      scope = $rootScope.$new()
    )


  setUDCData = (UDC) ->
    UDC.set('store_id',   'test-store-id')
    UDC.set('neo4j_version', 'Neo4j browser test version')

  describe "User opt in/out", ->
    it 'should not ping unless "shouldReportUdc" in Settings is set to true', ->
      setUDCData UsageDataCollectionService
      not_defined = UsageDataCollectionService.shouldPing 'connect'
      Settings.shouldReportUdc = no
      set_false = UsageDataCollectionService.shouldPing 'connect'
      Settings.shouldReportUdc = yes
      set_true = UsageDataCollectionService.shouldPing 'connect'

      expect(not_defined).toBeFalsy()
      expect(set_false).toBe(false)
      expect(set_true).toBe(true)

  describe "Ping frequency", ->
    it 'should not ping twice', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      setUDCData UsageDataCollectionService

      set_true = UsageDataCollectionService.shouldPing 'connect'
      too_soon = UsageDataCollectionService.shouldPing 'connect'

      expect(set_true).toBe(true)
      expect(too_soon).toBe(false)

  describe "Required data", ->
    it 'should not ping until required data is present', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      no_data = UsageDataCollectionService.shouldPing 'connect'

      setUDCData UsageDataCollectionService
      has_data = UsageDataCollectionService.shouldPing 'connect'

      expect(no_data).toBe(false)
      expect(has_data).toBe(true)

  describe "Stats", ->
    it 'should treat a begin/commit transaction as one attempt', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      setUDCData UsageDataCollectionService
      current_transaction = Cypher.transaction()
      httpBackend.expectPOST("#{Settings.endpoint.transaction}")
      .respond(->
        return [200, JSON.stringify({
          commit: "http://localhost:9000#{Settings.endpoint.transaction}/1/commit",
          results: []
          errors: []
        })]
      )

      current_transaction.begin().then(
        ->
          type = typeof UsageDataCollectionService.data['cypher_attempts']
          expect(type).toBe('undefined')
      )
      scope.$apply()
      httpBackend.flush()

      httpBackend.expectPOST("#{Settings.endpoint.transaction}/1/commit")
      .respond(->
        return [200, JSON.stringify({
          results: [{
            columns: ["n"],
            data: [{
              row: [{name: 'mock'}],
              graph: {nodes: [{id: 1}], relationships: []}
            }],
            stats: {}
          }],
          errors: []
        })]
      )
      current_transaction.commit("MATCH n RETURN n").then(
        ->
          expect(UsageDataCollectionService.data.cypher_attempts).toBe(1)
          expect(UsageDataCollectionService.data.cypher_wins).toBe(1)
      )
      scope.$apply()
      httpBackend.flush()

    it 'should collect stats from auto-committed transactions', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      setUDCData UsageDataCollectionService
      current_transaction = Cypher.transaction()

      httpBackend.expectPOST("#{Settings.endpoint.transaction}/commit")
      .respond(->
        return [200, JSON.stringify({
          results: [{
            columns: ["n"],
            data: [{
              row: [{name: 'mock'}],
              graph: {nodes: [{id: 1}], relationships: []}
            }],
            stats: {}
          }],
          errors: []
        })]
      )
      current_transaction.commit("MATCH n RETURN n").then(
        ->
          expect(UsageDataCollectionService.data.cypher_attempts).toBe(1)
          expect(UsageDataCollectionService.data.cypher_wins).toBe(1)
      )
      scope.$apply()
      httpBackend.flush()

    it 'should detect a cypher failure', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      setUDCData UsageDataCollectionService
      current_transaction = Cypher.transaction()
      httpBackend.expectPOST("#{Settings.endpoint.transaction}")
      .respond(->
        return [200, JSON.stringify({
          commit: "http://localhost:9000#{Settings.endpoint.transaction}/1/commit",
          results: []
          errors: []
        })]
      )

      current_transaction.begin()
      scope.$apply()
      httpBackend.flush()

      httpBackend.expectPOST("#{Settings.endpoint.transaction}/1/commit")
      .respond(->
        return [200, JSON.stringify({
          results: [],
          errors: [{code: 'TestFail', message:'This is a wanted failure.'}]
        })]
      )
      current_transaction.commit("MATCH n RETURN nr").then(
        ->
          expect("This should never happen").toBe('Nope')
        ->
          wins_type = typeof UsageDataCollectionService.data['cypher_wins']
          expect(wins_type).toBe('undefined')
          expect(UsageDataCollectionService.data.cypher_attempts).toBe(1)
          expect(UsageDataCollectionService.data.cypher_fails).toBe(1)
      )
      scope.$apply()
      httpBackend.flush()

    it 'should detect a failure in an auto-committed transaction', ->
      UsageDataCollectionService.reset()
      Settings.shouldReportUdc = yes
      setUDCData UsageDataCollectionService
      current_transaction = Cypher.transaction()

      httpBackend.expectPOST("#{Settings.endpoint.transaction}/commit")
      .respond(->
        return [200, JSON.stringify({
          results: [],
          errors: [{code: 'TestFail', message:'This is a wanted failure.'}]
        })]
      )
      current_transaction.commit("MATCH n RETURN n").then(
        ->
          expect("This should never happen").toBe('Nope')
        ->
          wins_type = typeof UsageDataCollectionService.data['cypher_wins']
          expect(wins_type).toBe('undefined')
          expect(UsageDataCollectionService.data.cypher_attempts).toBe(1)
          expect(UsageDataCollectionService.data.cypher_fails).toBe(1)
      )
      scope.$apply()
      httpBackend.flush()

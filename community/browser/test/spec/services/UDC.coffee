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

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach ->
    inject((_UsageDataCollectionService_, _Settings_) ->
      UsageDataCollectionService = _UsageDataCollectionService_
      Settings = _Settings_
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


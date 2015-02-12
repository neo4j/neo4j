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

describe 'Service: motdService', () ->
  motdService = {}
  mockJSON = '[{"a":"neo4jmotd","d":"General, not important","n":"","u":"http:\\/\\/notimportant","t":"","dt":"2014-11-04T00:00:57Z"},{"a":"neo4jmotd","d":"Important, targeted ad 2.1.X","n":"","u":"http:\\/\\/2.1.X","t":["version=2\\\\.1\\\\.\\\\d","level=debug","!important","foo=bar","attempt=5","plain_tag","combo=!bang"],"dt":"2014-10-14T14:20:38Z"},{"a":"neo4jmotd","d":"Important, targeted 2.2","n":"","u":"http:\\/\\/2.2","t":["version=2\\\\.2\\\\.\\\\d","level=debug","!important","foo=bar","attempt=5","plain_tag","combo=!bang"],"dt":"2014-10-14T14:20:38Z"}]'
  mockData = JSON.parse mockJSON
  beforeEach ->
    module 'neo4jApp.services'

  beforeEach ->
    inject((_motdService_) ->
      motdService = _motdService_

    )

  describe ' - Get correct message depending on version', ->

    it ' - Version 2.0.1', ->
      motdService.setCallToActionVersion "2.0.1"
      item = motdService.getCallToActionFeedItem mockData
      expect(item.u).toBe "http://notimportant"

    it ' - Version 2.1.6', ->
      motdService.setCallToActionVersion "2.1.6"
      item = motdService.getCallToActionFeedItem mockData
      expect(item.u).toBe "http://2.1.X"

    it ' - Version 2.1.6-SNAPSHOT', ->
      motdService.setCallToActionVersion "2.1.6-SNAPSHOT"
      item = motdService.getCallToActionFeedItem mockData
      expect(item.u).toBe "http://2.1.X"

    it ' - Version 2.2.0-M01', ->
      motdService.setCallToActionVersion "2.2.0-M01"
      item = motdService.getCallToActionFeedItem mockData
      expect(item.u).toBe "http://2.2"

    it ' - Version 2.2.0', ->
      motdService.setCallToActionVersion "2.2.0"
      item = motdService.getCallToActionFeedItem mockData
      expect(item.u).toBe "http://2.2"

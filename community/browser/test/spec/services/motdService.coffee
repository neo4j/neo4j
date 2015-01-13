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

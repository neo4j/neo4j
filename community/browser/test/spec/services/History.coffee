'use strict'

describe 'Service: HistoryService', ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach -> localStorage.clear()

  # instantiate service
 
  Settings = {}
  HistoryService = {}
  beforeEach inject (_HistoryService_, _Settings_) ->
    HistoryService = _HistoryService_
    Settings = _Settings_

  describe '#add', ->
    it 'should add an item to the history', ->
      HistoryService.add('an item')
      expect(HistoryService.history[0]).toBe('an item')

  describe '#addTooMany', ->
    it 'should not hold more than Settings.maxHistory', ->
      for i in [0...Settings.maxHistory]
        HistoryService.add("Item #{i}")
      HistoryService.add("This is too much. Pop should happen.")
      expect(HistoryService.history.length).toBe(Settings.maxHistory)

  describe '#setBuffer', ->
    it 'should hold a buffer when stepping', ->
      HistoryService.add('1')
      HistoryService.add('2')
      HistoryService.setBuffer('11')
      item = HistoryService.prev()
      expect(item).toBe('2')
      item = HistoryService.prev()
      expect(item).toBe('1')
      item = HistoryService.next()
      item = HistoryService.next()
      expect(item).toBe('11')
      expect(HistoryService.history.length).toBe(2)

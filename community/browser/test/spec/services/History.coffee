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

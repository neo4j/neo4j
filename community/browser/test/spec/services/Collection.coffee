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

describe 'Service: Collection', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  items = null

  # instantiate service
  Collection = {}
  ModelCollection = {}
  Document = {}
  beforeEach inject (_Collection_, _Document_) ->
    Document = _Document_
    Collection = new _Collection_(null)
    ModelCollection = new _Collection_(null, _Document_.klass)

  describe 'add:', ->
    it 'should be able to add single item', () ->
      Collection.add(1)
      expect(Collection.all().length).toBe 1

    it 'should be able to add an array of items', () ->
      Collection.add([1, 2, 3])
      expect(Collection.all().length).toBe 3

    it "should return the collection", ->
      item = {id: 1}
      expect(Collection.add(item)).toBe Collection

    it 'should convert object to a model if defined', ->
      item = {id: 1}
      ModelCollection.add(item)
      expect(ModelCollection.get(1) instanceof Document.klass).toBe true

  describe 'first:', ->
    beforeEach ->
      Collection.add([{id: 2}, {id: 3}, {id: 1}])

    it 'should retrieve first item', ->
      expect(Collection.first().id).toBe 2

  describe 'get:', ->
    beforeEach ->
      Collection.add([{id: 1, name: 'shoe'}, {id: 2, name: 'tie'}])

    it 'should be able to retrieve item by id', ->
      item = Collection.get(1)
      expect(item.name).toBe 'shoe'

    it 'should be able to retrieve item by reference', ->
      item = Collection.get(1)
      item_copy = Collection.get(item)
      expect(item_copy).toBe item

    it 'should be able to retrieve and item without an id', ->
      item = "Hello"
      Collection.add(item)
      item_copy = Collection.get(item)
      expect(item_copy).toBe item

    it 'should be able to handle undefined input', ->
      expect(Collection.get).not.toThrow()

    it 'should return undefined on non numeric input', ->
      expect(Collection.get('hello')).toBe undefined

    it 'should return undefined on non existant id', ->
      expect(Collection.get(3)).toBe undefined

    it 'should allow both numerical and string based ID', ->
      Collection.add({id: 'identifier'})
      expect(Collection.get('identifier').id).toBe 'identifier'

  describe 'indexOf:', ->
    it 'should tell index of an element', ->
      item = {id: 3}
      Collection.add [{id: 1}, {id: 2}, item]
      expect(Collection.indexOf(item)).toBe 2

  describe 'last:', ->
    beforeEach ->
      Collection.add([{id: 2}, {id: 3}, {id: 1}])

    it 'should retrieve last item', ->
      expect(Collection.last().id).toBe 1

  describe 'next:', ->
    beforeEach ->
      Collection.add([{id: 2}, {id: 3}, {id: 1}])

    it 'should retrieve the next item', ->
      item = Collection.get(2)
      expect(Collection.next(item).id).toBe 3

    it 'should wrap to first element if asking for next element from last', ->
      item = Collection.get(1)
      expect(Collection.next(item)).toBe undefined

  describe 'remove:', ->
    beforeEach ->
      items = Collection.add([{id: 2}, {id: 3}, {id: 1}])
    it 'should be able to remove and item by reference', ->
      len = Collection.all().length
      item = Collection.get 1
      Collection.remove item
      expect(Collection.all().length).toBe len-1

    it 'should return the element being removed', ->
      item = Collection.get(1)
      removed = Collection.remove(item)
      expect(removed).toBe item

  describe 'pluck:', ->
    beforeEach ->
      Collection.add([{id: 1}, {id: 2}, {id: 3}])
    it 'should return a list of given item attribute', ->
      attrs = Collection.pluck('id')
      expect(attrs.length).toBe 3
      expect(attrs).toContain attr for attr in [1...3]

  describe 'prev:', ->
    beforeEach ->
      Collection.add([{id: 2}, {id: 3}, {id: 1}])

    it 'should retrieve the previous item', ->
      item = Collection.get(3)
      expect(Collection.prev(item).id).toBe 2

    it 'should wrap to last element if asking for previous element from first', ->
      item = Collection.get(2)
      expect(Collection.prev(item)).toBe undefined

  describe 'where:', ->
    beforeEach ->
      Collection.add([{id: 1, name: 'shoe'}, {id: 2, name: 'tie'}, {id: 3, name: 'tie'}])

    it 'should return a list of items that matches provided attrbutes', ->
      items = Collection.where({name: 'tie'})
      expect(items.length).toBe 2

    it 'should return a list of items matching several provided attrbutes', ->
      items = Collection.where({name: 'tie', id: 3})
      expect(items.length).toBe 1

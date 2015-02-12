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

describe 'Service: Folder', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Document = {}
  Folder = {}
  beforeEach inject (_Document_, _Folder_) ->
    Document = _Document_
    Folder = _Folder_
    Document.reset([]).save()
    Folder.reset([]).save()

  it 'should be expanded by default', ->
    folder = Folder.new()
    expect(folder.expanded).toBeTruthy()

  it 'should get an id even if not specified', ->
    folder = Folder.new()
    expect(folder.id).toBeTruthy()

  describe '#create', ->
    it 'should add a folder to the collection', ->
      len = Folder.length
      Folder.create()
      expect(Folder.length).toBe len+1

    it 'should return the created folder', ->
      folder = Folder.create()
      expect(folder instanceof Folder.klass).toBeTruthy()

  describe '#destroy', ->
    it 'should destroy a folder from the collection', ->
      f = Folder.create()
      len = Folder.length
      Folder.destroy(f)
      expect(Folder.length).toBe len-1

    it 'should destroy all documents within a folder', ->
      f = Folder.create(id: 'test')
      d = Document.create(folder: 'test')
      expect(Document.where(folder: 'test').length).toBe 1

      Folder.destroy(f)
      expect(Document.where(folder: 'test').length).toBe 0

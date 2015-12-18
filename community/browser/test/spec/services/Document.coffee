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

describe 'Service: Document', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Document = {}
  beforeEach inject (_Document_) ->
    Document = _Document_

  it 'should not belong to any folder when created with default options', ->
    doc = Document.new()
    expect(doc.folder).toBeFalsy()

  it 'should set the specified folder when created', ->
    doc = Document.new(folder: 'examples')
    expect(doc.folder).toBe 'examples'

  it 'should get an id even if not specified', ->
    doc = Document.new()
    expect(doc.id).toBeTruthy()

  describe '#create', ->
    it 'should add a document to the collection', ->
      len = Document.length
      Document.create()
      expect(Document.length).toBe len+1

    it 'should return the document being created', ->
      d = Document.create()
      expect(d instanceof Document.klass).toBeTruthy()

  describe '#destroy', ->
    it 'should destroy a document from the collection', ->
      f = Document.create()
      len = Document.length
      Document.destroy(f)
      expect(Document.length).toBe len-1

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

  describe '#remove', ->
    it 'should remove a document from the collection', ->
      f = Document.create()
      len = Document.length
      Document.remove(f)
      expect(Document.length).toBe len-1

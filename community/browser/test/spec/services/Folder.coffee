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

  describe '#remove', ->
    it 'should remove a folder from the collection', ->
      f = Folder.create()
      len = Folder.length
      Folder.remove(f)
      expect(Folder.length).toBe len-1

    it 'should remove all documents within a folder', ->
      f = Folder.create(id: 'test')
      d = Document.create(folder: 'test')
      expect(Document.where(folder: 'test').length).toBe 1

      Folder.remove(f)
      expect(Document.where(folder: 'test').length).toBe 0

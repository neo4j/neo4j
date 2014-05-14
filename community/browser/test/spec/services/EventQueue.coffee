'use strict'

describe 'Service: EventQueue', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  EventQueue = {}
  Folder = {}
  Document = {}
  beforeEach inject (_EventQueue_, _Folder_, _Document_) ->
    EventQueue = _EventQueue_
    Folder = _Folder_
    Document = _Document_

  it 'should create a folder', ->
    spyOn(Folder, 'create');
    EventQueue.trigger('folder.create')
    expect(Folder.create).toHaveBeenCalled();

  it 'should remove a folder', ->
    spyOn(Folder, 'remove');
    EventQueue.trigger('folder.remove')
    expect(Folder.remove).toHaveBeenCalled();

  it 'should create a document', ->
    spyOn(Document, 'create');
    EventQueue.trigger('document.create')
    expect(Document.create).toHaveBeenCalled();

  it 'should remove a document', ->
    spyOn(Document, 'remove');
    EventQueue.trigger('document.remove')
    expect(Document.remove).toHaveBeenCalled();

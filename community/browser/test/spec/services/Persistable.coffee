'use strict'

describe 'Service: Persistable', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Persistable = {}
  Storage = {}
  beforeEach inject (_Persistable_, _localStorageService_) ->
    Persistable = _Persistable_
    Storage = _localStorageService_
    spyOn(Storage, 'get')
    spyOn(Storage, 'add')

  it 'should generate an id when not specified', ->
    doc = new Persistable()
    expect(doc.id).toBeTruthy()

  it 'should set the provided id', ->
    doc = new Persistable(id: 1)
    expect(doc.id).toBe 1

  it 'should fetch from storage based on storageKey', ->
    class MyClass extends Persistable
      @storageKey: 'myclass'

    MyClass.fetch()
    expect(Storage.get).toHaveBeenCalledWith('myclass')

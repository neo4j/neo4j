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

  it 'should add timestamps object to newly created objects', ->
    doc = new Persistable()
    now = (new Date()).getTime()
    expect(doc.timestamps).toEqual(jasmine.any(Object))
    expect(doc.timestamps.created_at).toBeCloseTo(now)
    expect(doc.timestamps.updated_at).toBeCloseTo(now)

  it 'should set created_at/updated_at keys with provided data for new objects', ->
    now = 1234
    doc = new Persistable(timestamps: {
      created_at: now
      updated_at: now
    })
    expect(doc.timestamps.created_at).toEqual(now)
    expect(doc.timestamps.updated_at).toEqual(now)

  describe '#update', ->
    it 'should update updated_at with current time', ->
      doc = new Persistable(timestamps: {
        updated_at: 0
      })
      now = (new Date()).getTime()
      doc.update(title: 'hello')
      expect(doc.timestamps.updated_at).toBeCloseTo(now)

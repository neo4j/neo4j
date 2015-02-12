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

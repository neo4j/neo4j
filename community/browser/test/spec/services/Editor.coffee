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

describe 'Service: Editor', ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  beforeEach -> localStorage.clear()

  # instantiate service
  Document = {}
  Editor = {}
  Settings = {}
  $timeout = null
  $httpBackend = null
  beforeEach inject (_Document_, _Editor_, _Settings_, _$timeout_, _$httpBackend_) ->
    Document = _Document_
    Editor = _Editor_
    Settings = _Settings_
    $timeout = _$timeout_
    $httpBackend = _$httpBackend_
    Document.reset([{
      id: 1
      content: 'test content'
    }])

  describe '#execScript', ->
    it 'does not create more history items than allowed by Settings', ->
      for i in [0..Settings.maxHistory]
        Editor.addToHistory("command " + i)

      Editor.addToHistory('new command')
      expect(Editor.history.history.length).toBe Settings.maxHistory
      expect(Editor.history.history[0]).toBe 'new command'


  describe '#loadDocument', ->
    beforeEach ->
      Editor.loadDocument(1)

    it 'should load content from a document into editor', ->
      expect(Editor.content).toBe 'test content'

    it 'should load document ID from a document', ->
      expect(Editor.document.id).toBe 1

  describe '#saveDocument', ->
    it 'should not create a new document with blank input', ->
      len = Document.length
      Editor.content = '    '
      Editor.saveDocument()
      expect(Document.length).toBe 1

    it 'should create a new document if no document was loaded', ->
      len = Document.length
      Editor.content = 'second document'
      Editor.saveDocument()
      expect(Document.length).toBe len+1

    it 'should set document ID after a new document was created', ->
      Editor.content = "new document"
      Editor.saveDocument()
      expect(Editor.document).toBeTruthy()

    it 'should update an existing document if it was loaded', ->
      len = Document.length
      Editor.loadDocument 1
      Editor.content = "updated document"
      Editor.saveDocument()
      expect(Document.length).toBe 1
      expect(Document.get(1).content).toBe('updated document')

    it 'should create a new document if the current lacks an id', ->
      len = Document.length
      Editor.loadDocument 1
      Document.destroy(Editor.document)
      expect(Document.length).toBe len-1
      Editor.saveDocument()
      expect(Document.length).toBe len

  describe '#hasChanged', ->
    it 'is not changed when no script is loaded', ->
      expect(Editor.hasChanged()).toBeFalsy()
    it 'is not changed when script is loaded', ->
      Editor.loadDocument 1
      expect(Editor.hasChanged()).toBeFalsy()
    it 'is changed when script is loaded and changed', ->
      Editor.loadDocument 1
      Editor.content = 'new content'
      expect(Editor.hasChanged()).toBeTruthy()

  describe '#historySet', ->
    it 'should clear the current document id', ->
      Editor.history.history = ['first', 'second']
      Editor.loadDocument 1
      expect(Editor.document).toBeTruthy()
      Editor.historySet(0)
      expect(Editor.document).toBeFalsy()

  describe '#setContent', ->
    beforeEach ->
      $httpBackend.when('JSONP', 'http://assets.neo4j.org/v2/json/neo4jmotd?callback=JSON_CALLBACK&count=10?plain=true').respond('')
    it 'should set the content', ->
      Editor.setContent 'hello'
      $timeout.flush()
      expect(Editor.content).toBe 'hello'
    it 'should clear the current document ID', ->
      Editor.loadDocument 1
      Editor.setContent 'hello'
      expect(Editor.document).toBeFalsy()

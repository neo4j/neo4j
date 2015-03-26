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

'use strict';

angular.module('neo4jApp.services')
  .service 'Editor', [
    'Document'
    'Frame'
    'Settings'
    'HistoryService'
    'motdService'
    '$timeout'
    (Document, Frame, Settings, HistoryService, motdService, $timeout) ->
      class Editor
        constructor: ->
          @history = HistoryService
          @content = ''
          @document = null

        execScript: (input, no_duplicates = false) ->
          @showMessage = no

          if no_duplicates 
            frame = Frame.createOne(input: input)
            return unless frame
          else
            frame = Frame.create(input: input)

          if !frame and input != ''
            @setMessage("<b>Unrecognized:</b> <i>#{input}</i>.", 'error')
          else
            @addToHistory(input) unless (Settings.filemode and @document?.id)
            @maximize(no)
            @document = null

        execCurrent: ->
          @execScript(@content)

        hasChanged:->
          @document?.content and @document.content isnt @content

        historyNext: ->
          @history.setBuffer(@content)
          item = @history.next()
          @setContent(item)

        historyPrev: ->
          @history.setBuffer(@content)
          item = @history.prev()
          @setContent(item)

        historySet: (idx) ->
          item = @history.get(idx)
          @setContent(item)

        addToHistory: (input) ->
          item = @history.add(input)
          @content = item

        loadDocument: (id) ->
          doc = Document.get(id)
          return unless doc
          @content = doc.content
          @document = doc

        maximize: (state = !@maximized) ->
          @maximized = !!state

        saveDocument: ->
          input = @content.trim()
          return unless input
          # re-fetch document from collection
          @document = Document.get(@document.id) if @document?.id
          if @document?.id
            @document.content = input
            Document.save()
          else
            @document = Document.create(content: @content)

        createDocument: (content = '// Untitled script\n', folder) ->
          @content = content
          @document = Document.create(content: content, folder: folder)

        cloneDocument: ->
          folder = @document?.folder
          @createDocument(@content, folder)

        setContent: (content = '') ->
          $timeout(=>
            @content = content
          ,0)
          @document = null

        setMessage: (message, type = 'info') ->
          @showMessage = yes
          @errorCode = type
          @errorMessage = message

      editor = new Editor()

      # Configure codemirror
      CodeMirror.commands.handleEnter = (cm) ->
        if cm.lineCount() == 1 and !editor.document
          editor.execCurrent()
        else
          CodeMirror.commands.newlineAndIndent(cm)

      CodeMirror.commands.handleUp = (cm) ->
        if cm.lineCount() == 1
          editor.historyPrev()
        else
          CodeMirror.commands.goLineUp(cm)

      CodeMirror.commands.handleDown = (cm) ->
        if cm.lineCount() == 1
          editor.historyNext()
        else
          CodeMirror.commands.goLineDown(cm)

      CodeMirror.commands.historyPrev = (cm) ->
        editor.historyPrev()
      CodeMirror.commands.historyNext = (cm) ->
        editor.historyNext()

      CodeMirror.commands.execCurrent = (cm) ->
        editor.execCurrent()

      CodeMirror.keyMap["default"]["Enter"] = "handleEnter"
      CodeMirror.keyMap["default"]["Shift-Enter"] = "newlineAndIndent"

      CodeMirror.keyMap["default"]["Cmd-Enter"] = "execCurrent"
      CodeMirror.keyMap["default"]["Ctrl-Enter"] = "execCurrent"
      
      CodeMirror.keyMap["default"]["Up"] = "handleUp"
      CodeMirror.keyMap["default"]["Down"] = "handleDown"
      CodeMirror.keyMap["default"]["Cmd-Up"] = "historyPrev"
      CodeMirror.keyMap["default"]["Ctrl-Up"] = "historyPrev"
      CodeMirror.keyMap["default"]["Cmd-Down"] = "historyNext"
      CodeMirror.keyMap["default"]["Ctrl-Down"] = "historyNext"

      editor
  ]

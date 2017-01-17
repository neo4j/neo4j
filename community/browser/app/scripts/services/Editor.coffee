###!
Copyright (c) 2002-2017 "Neo Technology,"
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
    'CypherParser'
    'motdService'
    '$timeout'
    (Document, Frame, Settings, HistoryService, CypherParser, motdService, $timeout) ->
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

        checkCypherContent: (cm) ->
          cb = (err, res) ->
            cm.clearGutter 'cypher-hints'
            return if err
            if res.raw.response.data.notifications?.length
              for item in res.raw.response.data.notifications
                if not item.position
                  item.position = {line:1}
                do(item) ->
                  cm.setGutterMarker(item.position.line-1, "cypher-hints", (()->
                      r = document.createElement("div")
                      r.style.color = "#822"
                      r.innerHTML = "<i class='fa fa-exclamation-triangle gutter-warning'></i>"
                      r.title = item.title + "\n" + item.description
                      r.onclick = ->
                        Frame.create({input:"EXPLAIN #{input}", showCypherNotification: yes})
                      return r)()
                  )
          input = cm.getValue()
          CypherParser.runHints cm, cb


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

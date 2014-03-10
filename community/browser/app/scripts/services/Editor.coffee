###!
Copyright (c) 2002-2014 "Neo Technology,"
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
    'EventQueue'
    'Frame'
    'Settings'
    'localStorageService'
    'motdService'
    'Utils'
    (Document, EventQueue, Frame, Settings, localStorageService, motdService, Utils) ->
      storageKey = 'history'
      class Editor
        constructor: ->
          @history = localStorageService.get(storageKey)
          @history = [] unless angular.isArray(@history)
          @content = ''
          @current = ''
          @cursor = -1
          @document = null
          # @setMessage("#{motdService.quote.text}.")

        execScript: (input) ->
          @showMessage = no
          frame = Frame.create(input: input)
          doc = @document

          # Increase script play count and average run time
          # TODO: this probably shouldn't be handled here
          if doc?.id
            EventQueue.trigger('document.update.metrics', doc, {
              total_runs: (doc.metrics.total_runs or 0) + 1
            })
            frame?.then(=>
              {average_runtime, total_runs} = doc.metrics
              EventQueue.trigger('document.update.metrics', doc, {
                average_runtime: Utils.updateAverage(frame.runTime, average_runtime, total_runs-1)
              })
            )

          if !frame and input != ''
            @setMessage("<b>Unrecognized:</b> <i>#{input}</i>.", 'error')
          else
            if !(Settings.fileMode and @document?.id)
              @addToHistory(input)
            @maximize(no)

          return

        addToHistory: (input) ->
          @current = ''
          if input?.length > 0 and @history[0] isnt input
            @history.unshift(input)
            @history.pop() until @history.length <= Settings.maxHistory
            localStorageService.add(storageKey, JSON.stringify(@history))
          @historySet(-1)

        execCurrent: ->
          @execScript(@content)

        # ABK: seems like something the Editor should not be doing
        focusEditor: ->
          $('#editor textarea').focus()

        hasChanged:->
          @document?.content and @document.content.trim() isnt @content.trim()

        historyNext: ->
          idx = @cursor
          idx ?= @history.length
          idx--
          @historySet(idx)

        historyPrev: ->
          idx = @cursor
          idx ?= -1
          idx++
          @historySet(idx)

        historySet: (idx) ->
          # cache unsaved changes if moving away from the temporary buffer
          @current = @content if @cursor == -1 and idx != -1

          idx = -1 if idx < 0
          idx = @history.length - 1 if idx >= @history.length
          @cursor = idx
          item = @history[idx] or @current
          @content = item
          @document = null

        loadDocument: (id) ->
          # return if @hasChanged() && !confirm("Are you sure you want to throw away your changes?")
          doc = Document.get(id)
          return unless doc
          @content = doc.content
          @focusEditor()
          @document = doc

        maximize: (state = !@maximized) ->
          @maximized = !!state

        saveDocument: ->
          input = @content.trim()
          return unless input
          # re-fetch document from collection
          @document = Document.get(@document.id) if @document?.id
          if @document?.id
            EventQueue.trigger('document.update', @document, {
              content: input
            })
          else
            @document = EventQueue.trigger('document.create'
              content: @content, name: "title")

        setContent: (content = '') ->
          # return if @hasChanged() && !confirm("Are you sure you want to throw away your changes?")
          @content = content
          @focusEditor()
          @document = null

        setMessage: (message, type = 'info') ->
          @showMessage = yes
          @errorCode = type
          @errorMessage = message

      editor = new Editor()

      # Configure codemirror
      CodeMirror.commands.handleEnter = (cm) ->
        if cm.lineCount() == 1
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

      CodeMirror.keyMap["default"]["Enter"] = "handleEnter"
      CodeMirror.keyMap["default"]["Shift-Enter"] = "newlineAndIndent"
      CodeMirror.keyMap["default"]["Up"] = "handleUp"
      CodeMirror.keyMap["default"]["Down"] = "handleDown"

      editor
  ]

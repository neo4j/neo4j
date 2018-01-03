###
Copyright (c) 2002-2018 "Neo Technology,"
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

define(
  ['ribcage/View'
   'neo4j/webadmin/utils/Keys'
   'lib/amd/CodeMirror'
   #'neo4j/codemirror/cypher' # Disabled awaiting a test suite for it
   './consoleTemplate'
   'lib/amd/jQuery.putCursorAtEnd'], 
  (View, Keys, CodeMirror, template, $) ->

    class ConsoleView extends View
      
      template : template

      events : 
        "paste #data-console"         : "onPaste"
        "click #data-execute-console" : "onSearchClicked"

      initialize : (options)->
        @dataModel = options.dataModel
        
        @dataModel.bind("change:query", @onDataModelQueryChanged)

      render : =>
        $(@el).html template()

        # TODO: Check if there is a way to re-use this
        @_editor = CodeMirror($("#data-console").get(0),{
          value: @dataModel.getQuery()
          onKeyEvent: @onKeyEvent
          #mode: "text/x-cypher"
          mode : "text"
        })

        # WebDriver functional tests are unable to enter
        # text into the editor. Give them a global reference
        # to use programatically.
        if document? then document.dataBrowserEditor = @_editor

        @_adjustEditorHeightToNumberOfNewlines()
        @el
    
      focusOnEditor : =>
        if @_editor?
          @_editor.focus()
          
          # Select all
          start = {line:0,ch:0}
          end = {line:@_editor.lineCount()-1,ch:@_editor.getLine(@_editor.lineCount()-1).length}
          @_editor.setSelection(start, end)

      # Event handling

      onSearchClicked : (ev) =>
        @_executeQuery @_getEditorValue()

      onKeyEvent : (editor, ev) =>
        #ev = jQuery.Event(ev.type)
        switch ev.type
          when "keyup"    then @onKeyUp(ev)
          when "keypress" then @onKeyPress(ev)

      onKeyPress : (ev) =>
        if ev.which is Keys.ENTER and ev.ctrlKey or ev.which is 10 # WebKit
          ev.stop()
          @_executeQuery @_getEditorValue()

      onKeyUp : (ev) =>
        @_adjustEditorHeightToNumberOfNewlines()
        @_saveCurrentEditorContents()

      onPaste : (ev) =>
        # We don't have an API to access the text being pasted,
        # so we work around it by adding this little job to the
        # end of the js work queue.
        setTimeout( @_adjustEditorHeightToNumberOfNewlines, 0)
        setTimeout( @_saveCurrentEditorContents, 0)

      onDataModelQueryChanged : (ev) =>
        if @dataModel.getQuery() != @_getEditorValue()
          @render()

      # Internals

      _saveQueryInModel : (query) ->
        @dataModel.setQuery(query, false)

      _executeQuery : (query) ->
        @_saveQueryInModel(query)
        @dataModel.trigger("change:query")
        @dataModel.executeCurrentQuery()

      _adjustEditorHeightToNumberOfNewlines : =>
        @_setEditorLines @_newlinesIn(@_getEditorValue()) + 1

      _saveCurrentEditorContents : =>
        @_saveQueryInModel(@_getEditorValue())

      _setEditorLines : (numberOfLines) ->
        # TODO: Create single source of truth for line height here 
        # (eg. now it is both here and in style.css)
        height = 10 + 14 * numberOfLines
        $(".CodeMirror-scroll",@el).css("height",height)
        @_editor.refresh()

      _getEditorValue : ()  -> @_editor.getValue()
      _setEditorValue : (v) -> @_editor.setValue(v)

      _newlinesIn : (string) ->
        if string.match(/\n/g) then string.match(/\n/g).length else 0

)

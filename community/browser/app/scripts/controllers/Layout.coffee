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

'use strict'
#
# Handles UI state and current page
#
angular.module('neo4jApp.controllers')
  .controller 'LayoutCtrl', [
    '$rootScope'
    '$timeout'
    '$modal'
    'Editor'
    'Frame'
    'GraphStyle'
    'Utils'
    ($scope, $timeout, $modal, Editor, Frame, GraphStyle, Utils) ->

      _codeMirror = null
      dialog = null
      dialogOptions =
        backdrop: yes
        backdropClick: yes
        backdropFade: yes
        dialogFade: yes
        keyboard: yes
        size: 'lg'

      $scope.showDoc = () ->
        Frame.create(input: ':play')

      $scope.showStats = () ->
        Frame.create(input: ':schema')

      $scope.focusEditor = (ev) ->
        ev?.preventDefault()
        _codeMirror?.focus()

      $scope.codemirrorLoaded = (_editor) ->
        _codeMirror = _editor
        _codeMirror.focus()

        # Hack for initializing cursor properly in IE
        _codeMirror.setValue(' ')
        _codeMirror.setCursor(1, 1)
        _codeMirror.setValue('')

        _codeMirror.on "change", (cm) ->
          $scope.editorChanged(cm)
        _codeMirror.on "focus", (cm) ->
          $scope.editorChanged(cm)

      $scope.isEditorFocused = () ->
        $('.CodeMirror-focused').length > 0

      $scope.editor = Editor
      $scope.editorOneLine = true
      $scope.editorChanged = (codeMirror) ->
        $scope.editorOneLine = codeMirror.lineCount() == 1 and !Editor.document
        $scope.disableHighlighting = codeMirror.getValue().trim()[0] == ':'

      $scope.isDrawerShown = false
      $scope.whichDrawer = ""
      $scope.toggleDrawer = (selectedDrawer = "", state) ->
        state ?= !$scope.isDrawerShown or (selectedDrawer != $scope.whichDrawer)
        $scope.isDrawerShown = state
        $scope.whichDrawer = selectedDrawer

      $scope.showingDrawer = (named) ->
        $scope.isDrawerShown and ($scope.whichDrawer is named)

      $scope.$watch 'isDrawerShown', () ->
        $timeout(() -> $scope.$emit 'layout.changed', 0)

      $scope.isPopupShown = false
      $scope.togglePopup = (content) ->
        if content?
          if not dialog
            dialogOptions.templateUrl = 'popup-' + content
            dialogOptions.windowClass = 'modal-' + content
            dialog = $modal.open(dialogOptions)
            dialog.result.finally(->
              $scope.popupContent = null
              $scope.isPopupShown = no
              dialog = null
            )
        else
          dialog.close() if dialog?
          dialog = null


        $scope.popupContent = dialog
        $scope.isPopupShown = !!dialog

      $scope.globalKey = (e) ->
        resizeStream()

        return if $scope.isPopupShown and e.keyCode != 191

        # ABK: kinda weird as a global key.
        if (e.metaKey or e.ctrlKey) and e.keyCode is 13 # Cmd-Enter
          e.preventDefault()
          Editor.execCurrent()
        else if e.ctrlKey and e.keyCode is 38 # Ctrl-Up
          e.preventDefault()
          Editor.historyPrev()
        else if e.ctrlKey and e.keyCode is 40 # Ctrl-Down
          e.preventDefault()
          Editor.historyNext()
        else if e.keyCode is 27 # Esc
          if $scope.isPopupShown
            $scope.togglePopup()
          else
            Editor.maximize()
        else if e.keyCode is 191 # '/'
          unless $scope.isEditorFocused()
            e.preventDefault()
            $scope.focusEditor()
        # else
        #   console.debug(e)

      resizeStream = Utils.debounce((ignored) ->
        unless $scope.editor.maximized
          $('#stream').css
            'top': $('.view-editor').height() + $('.file-bar').height()
          $scope.$emit 'layout.changed'
      , 100)

      $(window).resize(resizeStream)

      $("body").bind "transitionend webkitTransitionEnd oTransitionEnd MSTransitionEnd", ->
        resizeStream()
  ]

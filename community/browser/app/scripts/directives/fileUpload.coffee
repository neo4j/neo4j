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
angular.module('neo4jApp.directives')
  .controller('fileUpload', [
    '$attrs'
    '$parse'
    '$rootScope'
    '$scope'
    '$window'
    ($attrs, $parse, $rootScope, $scope, $window) ->
      INITIAL_STATUS = $attrs.message or 'Drop Cypher script file to import'
      $scope.status = INITIAL_STATUS

      onUploadSuccess = (content, name)->
        if $attrs.upload
          exp = $parse($attrs.upload)
          $scope.$apply(->exp($scope, {'$content': content, '$name': name}))

      getFirstFileFromEvent = (evt) ->
        files = evt.originalEvent.dataTransfer.files
        return files[0]

      scopeApply = (fn)->
        return ->
          fn.apply($scope, arguments)
          $scope.$apply()

      @onDragEnter = scopeApply (evt)->
        getFirstFileFromEvent(evt)
        $scope.active = yes

      @onDragLeave = scopeApply ->
        $scope.active = no

      @onDrop = scopeApply (evt) =>
        @preventDefault(evt)
        $scope.active = no
        file = getFirstFileFromEvent(evt)
        return unless file

        # Match MIME type if requested
        if $attrs.type
          return if file.type.indexOf($attrs.type) < 0
        # Match file extension(s) if requested
        if $attrs.extension
          reg = new RegExp($attrs.extension + "$")
          if not file.name.match(reg)
            return alert("Only .#{$attrs.extension} files are supported")

        $scope.status = "Uploading..."
        @readFile file

      @preventDefault = (evt) ->
        evt.stopPropagation()
        evt.preventDefault()

      @readFile = (file) ->
        reader = new $window.FileReader()
        reader.onerror = scopeApply (evt) ->
          # http://www.w3.org/TR/FileAPI/#ErrorDescriptions
          $scope.status = switch evt.target.error.code
            when 1 then "#{file.name} not found."
            when 2 then "#{file.name} has changed on disk, please re-try."
            when 3 then "Upload cancelled."
            when 4 then "Cannot read #{file.name}"
            when 5 then "File too large for browser to upload."
          $rootScope.$broadcast 'fileUpload:error', $scope.error

        reader.onloadend = scopeApply (evt) ->
          data = evt.target.result
          data = data.split('base64,')[1]
          onUploadSuccess($window.atob(data), file.name)
          $scope.status = INITIAL_STATUS

        reader.readAsDataURL(file)

      return @
  ])


angular.module('neo4jApp.directives')
  .directive('fileUpload', [
    '$window'
    ($window) ->
      controller: 'fileUpload'
      restrict: 'E'
      scope: '@'
      transclude: yes
      template: '<div class="file-drop-area" ng-class="{active: active}" ng-bind="status"></div>'
      link: (scope, element, attrs, ctrl) ->
        return unless $window.FileReader and $window.atob
        element.bind 'dragenter', ctrl.onDragEnter
        element.bind 'dragleave', ctrl.onDragLeave
        element.bind 'drop', ctrl.onDrop
        element.bind 'dragover', ctrl.preventDefault
        element.bind 'drop'
  ])

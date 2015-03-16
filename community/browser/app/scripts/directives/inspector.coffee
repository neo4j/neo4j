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
  .directive('inspector', [
    '$dialog'
    ($dialog) ->
      restrict: 'EA'
      terminal: yes
      link: (scope, element, attrs) ->
        opts =
          backdrop: no
          dialogClass: 'inspector'
          dialogFade: yes
          keyboard: no
          template: element.html()
          resolve: { $scope: -> scope }

        dialog = $dialog.dialog(opts)
        dialog.backdropEl.remove()
        # Inherit position
        dialog.modalEl.css
          position: 'absolute'
          top: element.css('top')
          right: element.css('right')

        element.remove()

        shownExpr = attrs.inspector or attrs.show
        scope.$watch shownExpr, (val) ->
          if val
            dialog.open()
            dialog.modalEl.draggable?(handle: '.header')
          else
            dialog.close() if dialog.isOpen()
  ])

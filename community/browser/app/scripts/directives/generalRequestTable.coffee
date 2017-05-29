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

angular.module('neo4jApp.directives')
.directive('generalRequestTable', ['Utils', (Utils) ->
    replace: yes
    restrict: 'E'
    link: (scope, elm, attr) ->
      unbind = scope.$watch attr.tableData, (result) ->
        return unless result
        elm.html(render(result))
        unbind()

      render = (result) ->
        rows = result
        map = ['url', 'method', 'status']
        return "" unless Object.keys(rows).length
        html  = "<table class='table data'>"
        html += "<tbody>"
        for i, v of rows
          continue unless i in map
          html += "<tr>"
          html += '<td>' + Utils.escapeHTML(i) + '</td>'
          html += '<td>' + Utils.escapeHTML(v) + '</td>'
          html += "</tr>"
        html += "</tbody>"
        html += "</table>"
        html

  ])

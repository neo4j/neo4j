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
  .directive('neoTable', [->
      replace: yes
      restrict: 'E'
      link: (scope, elm, attr) ->
        entityMap =
          "&": "&amp;"
          "<": "&lt;"
          ">": "&gt;"
          '"': '&quot;'
          "'": '&#39;'
          "/": '&#x2F;'

        escapeHtml = (string) ->
          String(string).replace(/[&<>"'\/]/g, (s) -> entityMap[s])

        emptyMarker = ->
          '<em>(empty)</em>'

        unbind = scope.$watch attr.tableData, (result) ->
          return unless result
          elm.html(render(result))
          unbind()

        json2html = (obj) ->
          return emptyMarker() unless Object.keys(obj).length
          html  = "<table class='json-object'><tbody>"
          html += "<tr><th>#{k}</th><td>#{cell2html(v)}</td></tr>" for own k, v of obj
          html += "</tbody></table>"
          html

        cell2html = (cell) ->
          if angular.isString(cell)
            return emptyMarker() unless cell.length
            escapeHtml(cell)
          else if angular.isArray(cell)
            "["+((cell2html(el) for el in cell).join(', '))+"]"
          else if angular.isObject(cell)
            json2html(cell)
          else
            escapeHtml(JSON.stringify(cell))

        # Manual rendering function due to performance reasons
        # (repeat watchers are expensive)
        render = (result) ->
          rows = result.rows()
          cols = result.columns()
          return "" unless cols.length
          html  = "<table class='table data'>"
          html += "<thead><tr>"
          for col in cols
            html += "<th>#{col}</th>"
          html += "</tr></thead>"
          html += "<tbody>"
          if result.displayedSize
            for i in [0...result.displayedSize]
              html += "<tr>"
              for cell in rows[i]
                html += '<td>' + cell2html(cell) + '</td>'
              html += "</tr>"
          else # empty results
            html += "<tr>"
            for col in cols
              html += '<td>&nbsp;</td>'
            html += "</tr>"
          html += "</tbody>"
          html += "</table>"
          html

  ])

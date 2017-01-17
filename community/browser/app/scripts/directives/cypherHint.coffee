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
.directive('cypherHint', [ ->
    restrict: 'A'
    link: (scope, element, attrs) ->
      unbind = scope.$watch attrs.cypherHint, (val) ->
        return unless val
        if not val.position
          val.position = {line: 1, column: 1, offset: 0}
        inputArr = attrs.cypherInput.replace(/^\s*(EXPLAIN|PROFILE)\s*/, '').split("\n")
        if val.position.line is 1 and val.position.column is 1 and val.position.offset is 0
          outputArr = inputArr
        else
          outputArr = [inputArr[val.position.line-1]]
          outputArr.push (' ' for i in [0...(val.position.column)]).join('')+"^"
        element.text(outputArr.join("\n"))
        unbind()
  ])

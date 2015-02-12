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
  .directive('outputRaw', ['Settings', (Settings) ->
    restrict: 'A'
    link: (scope, element, attrs) ->
      unbind = scope.$watch attrs.outputRaw, (val) ->
        return unless val
        val = JSON.stringify(val, null, 2) unless angular.isString(val)
        # Try to truncate string at first newline after limit
        str = val.substring(0, Settings.maxRawSize)
        rest = val.substring(Settings.maxRawSize + 1)
        if rest
          rest = rest.split("\n")[0] or ''
          str += rest + "\n...\n<truncated output>\n\nPress download to see complete response"
        element.text(str)
        unbind()
  ])

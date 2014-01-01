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
  .factory 'CSV', [
    () ->
      class Serializer
        constructor: (opts = {}) ->
          @options = angular.extend(opts,
            delimiter: ','
          )

          @_output = ""
          @_columns = null

        append: (row) ->
          if not angular.isArray(row) and row.length is @_columns?.length
            throw 'CSV: Row must an Array of column size'

          @_output += "\n"
          @_output += (@_escape(cell) for cell in row).join(@options.delimiter)

        columns: (cols) ->
          return @_columns unless cols?
          throw 'CSV: Columns must an Array' unless angular.isArray(cols)
          @_columns = (@_escape(c) for c in cols)
          @_output = @_columns.join(@options.delimiter);

        output: -> @_output

        _escape: (string) ->
          string = JSON.stringify(string) unless angular.isString(string)
          if string.indexOf(@options.delimiter) > 0 or string.indexOf('"') >= 0
            string = '"' + string.replace(/"/g, '""') + '"'

          string

      return {
        Serializer: Serializer
      }
  ]

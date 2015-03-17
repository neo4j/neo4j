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
  .directive('exportable', [() ->
    restrict: 'A'
    controller: [
      '$scope',
      '$window',
      'CSV',
      ($scope, $window, CSV) ->

        saveAs = (data, filename, mime = "text/csv;charset=utf-8") ->
          if !!navigator.userAgent.match(/Version\/[\d\.]+.*Safari/)
            return alert('Exporting data is currently not supported in Safari. Please use another browser.')
          blob = new Blob([data], {type: mime});
          $window.saveAs(blob, filename);

        $scope.exportJSON = (data) ->
          return unless data
          saveAs(JSON.stringify(data), 'result.json')

        $scope.exportCSV = (data) ->
          return unless data
          csv = new CSV.Serializer()
          csv.columns(data.columns())
          for row in data.rows()
            csv.append(row)

          saveAs(csv.output(), 'export.csv')

        $scope.exportGraSS = (data) ->
          saveAs(data, 'graphstyle.grass')

    ]
  ])

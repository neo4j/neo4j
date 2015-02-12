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

# Requires jQuery
angular.module('neo4jApp.directives')
  .directive('serverTopic', ['$rootScope', 'Frame','Settings', ($rootScope, Frame, Settings) ->
    restrict: 'A'
    link: (scope, element, attrs) ->

      topic = attrs.serverTopic
      command = "server"

      if topic
        element.on 'click', (e) ->
          e.preventDefault()

          topic = topic.toLowerCase().trim()
          Frame.create(input: "#{Settings.cmdchar}#{command} #{topic}")

          $rootScope.$apply() unless $rootScope.$$phase

  ])

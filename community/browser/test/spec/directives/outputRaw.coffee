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

'use strict'

describe 'Directive: outputRaw', () ->
  beforeEach module 'neo4jApp.directives'


  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service

  element = {}

  it 'should only set the first content', inject ($rootScope, $compile) ->
    scope = $rootScope.$new()
    element = angular.element '<div output-raw="val"></div>'
    element = $compile(element)(scope)
    scope.val = "hello"
    scope.$apply()
    expect(element.text()).toBe 'hello'
    scope.val = "world"
    scope.$apply()
    expect(element.text()).toBe 'hello'


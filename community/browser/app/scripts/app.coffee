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

angular.module('neo4jApp.controllers', ['neo4jApp.utils'])
angular.module('neo4jApp.directives', ['ui.bootstrap.modal'])
angular.module('neo4jApp.filters', [])
angular.module('neo4jApp.services', ['LocalStorageModule', 'neo4jApp.settings', 'neo4jApp.utils', 'base64'])

app = angular.module('neo4jApp', [
  'ngAnimate'
  'neo4jApp.controllers'
  'neo4jApp.directives'
  'neo4jApp.filters'
  'neo4jApp.services'
  'neo4jApp.animations'
  'neo.exportable'
  'neo.csv'
  'ui.bootstrap.dropdown'
  'ui.bootstrap.position'
  'ui.bootstrap.tooltip'
  'ui.bootstrap.popover'
  'ui.bootstrap.tabs'
  'ui.bootstrap.carousel'
  'ui.codemirror'
  'ui.sortable'
  #'angularMoment'
  'ngSanitize'
])

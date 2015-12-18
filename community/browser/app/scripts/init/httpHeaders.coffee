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

angular.module('neo4jApp').config([
  '$httpProvider'
  ($httpProvider) ->
    $httpProvider.defaults.headers.common['X-stream'] = true
    $httpProvider.defaults.headers.common['Content-Type'] = 'application/json'

    $httpProvider.defaults.headers.get ||= {}
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache'
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache'
    $httpProvider.defaults.headers.get['If-Modified-Since'] = "Wed, 11 Dec 2013 08:00:00 GMT"
])

###!
Copyright (c) 2002-2016 "Neo Technology,"
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

class RssFeedService

  constructor: ($http) ->
    RssFeedService::get = () ->
      format = "json"
      username = "neo4jmotd"
      apiUrl = "#{document.location.protocol || 'http:'}//assets.neo4j.org/v2/#{format}/#{username}?callback=JSON_CALLBACK&count=10?plain=true"

      $http.jsonp(apiUrl)
      .error (results) ->
        return results
      .then (response) ->
        return [] unless response.data
        response.data

angular.module('neo4jApp.services').service 'rssFeedService', ['$http', RssFeedService]

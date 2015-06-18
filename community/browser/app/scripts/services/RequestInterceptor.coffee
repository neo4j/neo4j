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

angular.module('neo4jApp.services')
  .factory('RequestInterceptor', [
    'AuthDataService'
    (AuthDataService) ->
      interceptor =
        request: (config) ->
          isLocalRequest = yes
          if /^https?:/.test config.url
            url = document.location.origin || window.location.protocol + "//" + window.location.host
            if config.url.indexOf url < 0
              isLocalRequest = no

          return config if config.skipAuthHeader or not isLocalRequest
          header = AuthDataService.getAuthData()
          if header then config.headers['Authorization'] = "Basic #{header}"
          config
      interceptor
])

angular.module('neo4jApp.services').config(['$httpProvider', ($httpProvider) ->
  $httpProvider.interceptors.push 'RequestInterceptor'
])

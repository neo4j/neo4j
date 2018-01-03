###
Copyright (c) 2002-2018 "Neo Technology,"
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

define [], () ->

  class ItemUrlResolver

    constructor : (server) ->
      @server = server

    getNodeUrl : (id) =>
      @server.url + "/db/data/node/" + id

    getRelationshipUrl : (id) =>
      @server.url + "/db/data/relationship/" + id
      
    getNodeIndexHitsUrl: (index,key,value) =>
      @server.url + "/db/data/index" + index + "/" + ( encodeURIComponent key ) + "/" + ( encodeURIComponent value )
 
    extractNodeId : (url) =>
      @extractLastUrlSegment(url)

    extractRelationshipId : (url) =>
      @extractLastUrlSegment(url)

    extractLastUrlSegment : (url) =>
      if url.substr(-1) is "/"
        url = url.substr(0, url.length - 1)

      url.substr(url.lastIndexOf("/") + 1)

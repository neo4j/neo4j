###
Copyright (c) 2002-2011 "Neo Technology,"
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

define ["./ItemUrlResolver","lib/backbone"], (ItemUrlResolver) ->

  class NodeIndexSearcher

    constructor : (server) ->
      @server = server
      @urlResolver = new ItemUrlResolver(server)
      @pattern = /^node:index:"?(\w+)"?:"?(\w+)"?:"?([\w|\s]+)"?$/i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      data = @extractData(statement)
      hits = @server.index.getNodeIndex(data.index).exactQuery(data.key, data.value)
      console.log(hits)

    extractData : (statement) =>
      match = @pattern.exec(statement)
      index = match[1]
      key = match[2]
      value = match[3]
      return { index : index, key: key, value: value }
 

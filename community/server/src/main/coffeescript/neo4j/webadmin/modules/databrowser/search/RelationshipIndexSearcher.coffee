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

define ["neo4j/webadmin/utils/ItemUrlResolver"], (ItemUrlResolver) ->

  class NodeIndexSearcher

    constructor : (server) ->
      @server = server
      @urlResolver = new ItemUrlResolver(server)
      @pattern = /// ^ 
                    ((rel)|(relationship)):index:  # Start with rel:index or relationship:index
                    "?(\w+)"?:                     # Index name, optionally in quotes
                    (.+)                           # Query
                    $
                 ///i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      data = @extractData(statement)
      @server.index.getRelationshipIndex(data.index).query(data.query)

    extractData : (statement) =>
      match = @pattern.exec(statement)
      index = match[4]
      query = match[5]
      return { index : index, query:query }
 

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

define(
  ["./UrlSearcher", 
   "./NodeSearcher", 
   "./NodeIndexSearcher", 
   "./RelationshipSearcher",
   "./RelationshipsForNodeSearcher",
   "./RelationshipIndexSearcher",
   "./CypherSearcher"], 
  (UrlSearcher, NodeSearcher, NodeIndexSearcher, RelationshipSearcher, RelationshipsForNodeSearcher, RelationshipIndexSearcher, CypherSearcher) ->

    class Search

      constructor : (@server) ->
        
        @searchers = [
          new UrlSearcher(server)
          new NodeSearcher(server)
          new NodeIndexSearcher(server)
          new RelationshipSearcher(server)
          new RelationshipsForNodeSearcher(server)        
          new RelationshipIndexSearcher(server)
          new CypherSearcher(server)
        ]
      

      exec : (statement) =>
        searcher = @pickSearcher statement
        if searcher?
          searcher.exec statement
        else
          return neo4j.Promise.fulfilled(null)

      pickSearcher : (statement) =>
        for searcher in @searchers
          if searcher.match statement
            return searcher
)


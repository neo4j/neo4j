/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compatibility.LFUCache

/**
  * Cypher planner which has the ability to cache parsing and planning results.
  */
trait CachingPlanner[PARSED_QUERY <: AnyRef] {

  def parserCacheSize: Int

  private val parsedQueries = new LFUCache[String, PARSED_QUERY](parserCacheSize)

  /**
    * Get the parsed query from cache, or parses and caches it.
    *
    * @param preParsedQuery the pre-parsed query to get or cache
    * @param parser the parser to use if the query is not cached
    * @throws org.neo4j.cypher.SyntaxException if there are syntax errors
    * @return the parsed query
    */
  @throws(classOf[SyntaxException])
  protected def getOrParse(preParsedQuery: PreParsedQuery,
                           parser: => Parser[PARSED_QUERY]
                     ): PARSED_QUERY = {
    parsedQueries.get(preParsedQuery.statementWithVersionAndPlanner).getOrElse {
      val parsedQuery = parser.parse(preParsedQuery)
      parsedQueries.put(preParsedQuery.statementWithVersionAndPlanner, parsedQuery)
      parsedQuery
    }
  }

  /**
    * Clear the caches of this caching compiler.
    *
    * @return the number of entries that were cleared
    */
  def clearCaches(): Long = {
    parsedQueries.clear()
  }
}

trait Parser[PARSED_QUERY] {
  def parse(preParsedQuery: PreParsedQuery): PARSED_QUERY
}

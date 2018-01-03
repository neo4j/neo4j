/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.docgen.refcard

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.RefcardTest
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

class ReturnTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val title = "RETURN"
  val css = "read c3-3 c4-2 c5-3 c6-2"
  override val linkId = "query-return"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "all-nodes" =>
        assertStats(result, nodesDeleted = 0, relationshipsCreated = 0, propertiesSet = 0, relationshipsDeleted = 0)
        assert(result.toList.size === 4)
      case "alias" =>
        assertStats(result, nodesDeleted = 0, relationshipsCreated = 0, propertiesSet = 0, relationshipsDeleted = 0)
        assert(result.dumpToString.contains("columnName"))
      case "unique" =>
        assertStats(result, nodesDeleted = 0, relationshipsCreated = 0, propertiesSet = 0, relationshipsDeleted = 0)
        assert(result.toList.size === 1)
      case "skip" =>
        assertStats(result, nodesDeleted = 0)
        assert(result.toList.size === 3)
      case "skiplimit" =>
        assertStats(result, nodesDeleted = 0)
        assert(result.toList.size === 2)
      case "count" =>
        assert(result.toList.size === 1)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=limits" =>
        Map("limitNumber" -> 2, "skipNumber" -> 1)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "ROOT" -> Map("property" -> 0),
    "A" -> Map("property" -> 10),
    "B" -> Map("property" -> 20),
    "C" -> Map("property" -> 30))

  def text = """
###assertion=all-nodes
//
MATCH (n)

RETURN *###

Return the value of all identifiers.

### assertion=alias
MATCH (n)
WHERE id(n) = 1

RETURN n AS columnName###

Use alias for result column name.

### assertion=unique
MATCH (n)--(x)
WHERE id(x) in [%A%,%C%]
AND n.name = "B"

RETURN DISTINCT n###

Return unique rows.

###assertion=all-nodes
//
MATCH (n)
RETURN *

ORDER BY n.property
###

Sort the result.

###assertion=all-nodes
//
MATCH (n)
RETURN *

ORDER BY n.property DESC
###

Sort the result in descending order.

###assertion=skip parameters=limits
//
MATCH (n)
RETURN *

SKIP {skipNumber}
###

Skip a number of results.

###assertion=skiplimit parameters=limits
//
MATCH (n)
RETURN *

LIMIT {limitNumber}
###

Limit the number of results.

###assertion=skiplimit parameters=limits
//
MATCH (n)
RETURN *

SKIP {skipNumber} LIMIT {limitNumber}
###

Skip results at the top and limit the number of results.

###assertion=count
MATCH (n)

RETURN count(*)
###

The number of matching rows.
See Aggregation for more.
"""
}

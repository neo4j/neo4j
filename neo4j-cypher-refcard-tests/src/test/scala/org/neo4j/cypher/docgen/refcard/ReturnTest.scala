/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cypher.{ ExecutionResult, StatisticsChecker }
import org.neo4j.cypher.docgen.RefcardTest

class ReturnTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val section = "refcard"
  val title = "Return"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "all-nodes" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 4)
      case "alias" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.dumpToString.contains("columnName"))
      case "unique" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 1)
      case "skip" =>
        assertStats(result, deletedNodes = 0)
        assert(result.toList.size === 3)
      case "skiplimit" =>
        assertStats(result, deletedNodes = 0)
        assert(result.toList.size === 2)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=limits" =>
        Map("limit_number" -> 2, "skip_number" -> 1)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "ROOT" -> Map("propertyName" -> 0),
    "A" -> Map("propertyName" -> 10),
    "B" -> Map("propertyName" -> 20),
    "C" -> Map("propertyName" -> 30))

  def text = """.RETURN
["refcard", cardcss="read"]
----
###assertion=all-nodes
//
START n=node(*)

RETURN *###

Return the value of all identifiers.

### assertion=alias
START n=node(1)

RETURN n AS columnName###

Use alias for result column name.

### assertion=unique
START x=node(%A%,%C%)
MATCH n--x
WHERE n.name = "B"

RETURN DISTINCT n###

Return unique rows.

###assertion=all-nodes
//
START n=node(*)
RETURN *

ORDER BY n.propertyName
###

Sort the result.

###assertion=all-nodes
//
START n=node(*)
RETURN *

ORDER BY n.propertyName DESC
###

Sort the result in descending order.

###assertion=skip parameters=limits
//
START n=node(*)
RETURN *

SKIP {skip_number}
###

Skip a number of results.

###assertion=skiplimit parameters=limits
//
START n=node(*)
RETURN *

LIMIT {limit_number}
###

Limit the number of results.

###assertion=skiplimit parameters=limits
//
START n=node(*)
RETURN *

SKIP {skip_number} LIMIT {limit_number}
###

Skip results at the top and limit the number of results.
----
"""
}

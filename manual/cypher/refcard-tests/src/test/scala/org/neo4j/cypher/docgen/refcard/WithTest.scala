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

class WithTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT FRIEND A", "A FRIEND B", "B FRIEND C", "C FRIEND ROOT")
  val title = "WITH"
  val css = "read c2-2 c3-3 c4-2 c5-3 c6-2"
  override val linkId = "query-with"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "friends" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "with-limit" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 3)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=name" =>
        Map("name" -> "Andreas")
      case _ => Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Andreas"),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """
###assertion=friends parameters=name
//

MATCH (user)-[:FRIEND]-(friend)
WHERE user.name = {name}
WITH user, count(friend) AS friends
WHERE friends > 10
RETURN user

###

The `WITH` syntax is similar to `RETURN`.
It separates query parts explicitly, allowing you to declare which identifiers to carry over to the next part.

###assertion=with-limit
//

MATCH (user)-[:FRIEND]-(friend)
WITH user, count(friend) AS friends
ORDER BY friends DESC
SKIP 1 LIMIT 3
RETURN user
###

You can also use `ORDER BY`, `SKIP`, `LIMIT` with `WITH`.
"""
}

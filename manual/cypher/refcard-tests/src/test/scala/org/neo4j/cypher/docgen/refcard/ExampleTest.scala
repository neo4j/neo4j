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

import org.junit.Ignore
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.RefcardTest
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

@Ignore
class ExamplesTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT:Person FRIEND A:Person", "A:Person FRIEND B:Person", "B:Person FRIEND C:Person", "C:Person FRIEND ROOT:Person")
  val title = "Query Structure"
  val css = "general c2-2 c3-2 c4-2 c5-2"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "friends" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "create" =>
        assertStats(result, nodesCreated = 1, nodesDeleted = 1, propertiesSet = 3, labelsAdded = 1)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=name" =>
        Map("name" -> "Andreas", "city" -> "Malmö", "skipNumber" -> 10)
      case _ => Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Andreas"),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """
###assertion=friends parameters=name
//

MATCH (user:Person)-[:FRIEND]-(friend)
WHERE user.city = {city}
WITH user, count(friend) AS friendCount
WHERE friendCount > 10
RETURN user.name
ORDER BY friendCount DESC
SKIP {skipNumber}
LIMIT 10

###

A query that only reads data.
See the `WITH` section for additional options on its usage.

###assertion=create parameters=name
//

CREATE (user:Person {name: {name}})
SET user.city = {city}
FOREACH (n IN [user] : SET n.marked = true)
DELETE user
###

Basic write query.
Note that there can be multiple `CREATE`, `SET`, `FOREACH` or `DELETE` statements.

"""
}

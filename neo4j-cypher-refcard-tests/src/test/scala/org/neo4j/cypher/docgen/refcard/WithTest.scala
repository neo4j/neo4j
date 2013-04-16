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

class WithTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT FRIEND A", "A FRIEND B", "B FRIEND C", "C FRIEND ROOT")
  val section = "refcard"
  val title = "With"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "friends" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=name" =>
        Map("name" -> "Andreas")
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Andreas"),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """.WITH
["refcard", cardcss="read"]
----
###assertion=friends parameters=name
//

START user=node:nodeIndexName(name = {name})
MATCH user-[:FRIEND]-friend
WITH user, count(friend) as friends
WHERE friends > 10
RETURN user
###

The `WITH` syntax is similar to `RETURN`.
It separates query parts explicitly, allowing you to declare which identifiers to carry over to the next part.
----
"""
}

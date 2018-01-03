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

class CaseTest extends RefcardTest with QueryStatisticsTestSupport {
  def graphDescription = List(
    "A KNOWS B")
  val title = "CASE"
  val css = "general c2-1 c3-3 c4-2 c5-5 c6-4"
  override val linkId = "cypher-expressions"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "simple" =>
        assertStats(result, nodesCreated = 0)
        assert(!result.dumpToString().contains("3"))
      case "generic" =>
        assertStats(result, nodesCreated = 0)
        assert(!result.dumpToString().contains("3"))
    }
  }

  override val properties = Map(
    "A" -> Map[String, Any]("name" -> "Alice", "age" -> 38, "eyes" -> "brown"),
    "B" -> Map[String, Any]("name" -> "Beth", "age" -> 38, "eyes" -> "blue"))

  def text = """
###assertion=simple
MATCH n
RETURN

CASE n.eyes
 WHEN "blue" THEN 1
 WHEN "brown" THEN 2
 ELSE 3
END

AS result
###

Return `THEN` value from the matching `WHEN` value.
The `ELSE` value is optional, and substituted for `NULL` if missing.

###assertion=generic
MATCH n
RETURN

CASE
 WHEN n.eyes = "blue" THEN 1
 WHEN n.age < 40 THEN 2
 ELSE 3
END

AS result
###

Return `THEN` value from the first `WHEN` predicate evaluating to `TRUE`.
Predicates are evaluated in order.

"""
}

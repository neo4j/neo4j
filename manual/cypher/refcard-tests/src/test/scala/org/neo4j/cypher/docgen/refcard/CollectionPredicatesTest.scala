/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.InternalExecutionResult

class CollectionPredicatesTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "List Predicates"
  val css = "general c2-2 c3-3 c4-3 c5-2 c6-5"
  override val linkId = "query-predicates"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
    }
  }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> "Andrés"),
    "B" -> Map("property" -> "Tobias"),
    "C" -> Map("property" -> "Chris"))

  def text = """
###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
WITH nodes(path) AS coll, n, m
WHERE

all(x IN coll WHERE exists(x.property))

RETURN n,m###

Returns `true` if the predicate is `true` for all elements of the list.

###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
WITH nodes(path) AS coll, n, m
WHERE

any(x IN coll WHERE exists(x.property))

RETURN n, m###

Returns `true` if the predicate is `true` for at least one element of the list.

###assertion=returns-none
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
WITH nodes(path) AS coll, n, m
WHERE

none(x IN coll WHERE exists(x.property))

RETURN n, m###

Returns `true` if the predicate is `false` for all elements of the list.

###assertion=returns-none
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
WITH nodes(path) AS coll, n, m
WHERE

single(x IN coll WHERE exists(x.property))

RETURN n, m###

Returns `true` if the predicate is `true` for exactly one element in the list.
             """
}

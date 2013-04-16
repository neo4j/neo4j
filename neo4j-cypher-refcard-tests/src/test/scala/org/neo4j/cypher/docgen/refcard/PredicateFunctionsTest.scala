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

class PredicateFunctionsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "Predicate Functions"

  override def assert(name: String, result: ExecutionResult) {
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
    "A" -> Map("propertyName" -> "Andrés"),
    "B" -> Map("propertyName" -> "Tobias"),
    "C" -> Map("propertyName" -> "Chris"))

  def text = """.Predicate Functions
["refcard", cardcss="general"]
----
###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as collection, n, m
WHERE

ALL(x IN collection WHERE HAS(x.propertyName))

RETURN n,m###

Returns `true` if the predicate is `true` for all elements of the collection.

###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH collection=(n)-->(m)
WHERE

ANY(x IN collection WHERE HAS(x.propertyName))

RETURN n,m###

Returns `true` if the predicate is `true` for at least one element of the collection.

###assertion=returns-none
START n=node(%A%), m=node(%B%)
MATCH collection=(n)-->(m)
WHERE

NONE(x IN collection WHERE HAS(x.propertyName))

RETURN n,m###

Returns `true` if the predicate is `false` for all elements of the collection.

###assertion=returns-none
START n=node(%A%), m=node(%B%)
MATCH collection=(n)-->(m)
WHERE

SINGLE(x IN collection WHERE HAS(x.propertyName))

RETURN n,m###

Returns `true` if the predicate is `true` for exactly one element in the collection.
----
"""
}

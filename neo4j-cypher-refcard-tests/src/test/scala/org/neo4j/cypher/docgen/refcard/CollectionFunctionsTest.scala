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

class CollectionFunctionsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "Collection Functions"

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

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "parameters=range" =>
        Map("begin" -> 2, "end" -> 18, "step" -> 3)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("propertyName" -> "Andrés"),
    "B" -> Map("propertyName" -> "Tobias"),
    "C" -> Map("propertyName" -> "Chris"))

  def text = """.Collection Functions
[refcard]
----
###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
RETURN

NODES(path)
###

The nodes in the path.

###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
RETURN

RELATIONSHIPS(path)
###

The relationships in the path.

###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as collection
RETURN

EXTRACT(x IN collection: x.propertyName)
###

A collection of the value of the expression for each element in the collection.

###assertion=returns-one parameters=value
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as collection
RETURN

FILTER(x IN collection: x.propertyName <> {value})
###

A collection of the elements where the predicate is `true`.

###assertion=returns-one parameters=value
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as collection
RETURN

TAIL(collection)
###

All but the first element of the collection.

###assertion=returns-one parameters=range
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as collection
RETURN

RANGE({begin}, {end}, {step})
###

Numerical values in the range.
The `step` argument is optional.

###assertion=returns-one parameters=range
START n=node(%A%), m=node(%C%)
MATCH path=(n)-[*]->(m)
WITH nodes(path) as collection
RETURN

REDUCE(str = "", n IN collection : str + n.propertyName )
###

Evaluate expression for each element in the collection, accumulate the results.
----
"""
}

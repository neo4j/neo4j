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
  val graphDescription = List("ROOT KNOWS A", "A:Person KNOWS B:Person", "B KNOWS C:Person", "C KNOWS ROOT")
  val title = "Collection Functions"
  val css = "general c3-3 c4-3 c5-5 c6-6"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-three" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 3)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "friends" =>
        assertStats(result, nodesCreated = 0, propertiesSet = 2)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "parameters=range" =>
        Map("first" -> 2, "last" -> 18, "step" -> 3)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("prop" -> "Andrés"),
    "B" -> Map("prop" -> "Tobias"),
    "C" -> Map("prop" -> "Chris"))

  def text = """
###assertion=returns-three
MATCH (n:Person)
RETURN

LABELS(n)
###

The labels of the node.

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
WITH nodes(path) as coll
RETURN

EXTRACT(x IN coll : x.prop)
###

A collection of the value of the expression for each element in the collection.

###assertion=returns-one parameters=value
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as coll
RETURN

FILTER(x IN coll : x.prop <> {value})
###

A collection of the elements where the predicate is `true`.

###assertion=returns-one parameters=value
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as coll
RETURN

TAIL(coll)
###

All but the first element of the collection.

###assertion=returns-one parameters=range
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
WITH nodes(path) as coll
RETURN

RANGE({first}, {last}, {step})
###

Create a range of numbers.
The `step` argument is optional.

###assertion=returns-one parameters=range
START n=node(%A%), m=node(%C%)
MATCH path=(n)-[*]->(m)
WITH nodes(path) as coll
RETURN

REDUCE(str = "", n IN coll : str + n.prop)
###

Evaluate expression for each element in the collection, accumulate the results.

###assertion=friends
//
START begin = node(%A%), end = node(%B%)
MATCH path = begin -[*]-> end
WITH nodes(path) AS coll

FOREACH (n IN coll : SET n.marked = true)
###

Execute a mutating operation for each element in a collection.
"""
}

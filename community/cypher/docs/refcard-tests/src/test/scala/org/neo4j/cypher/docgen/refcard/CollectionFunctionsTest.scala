/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult

class CollectionFunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A:Person KNOWS B:Person", "B KNOWS C:Person", "C KNOWS ROOT")
  val title = "Collection Functions"
  val css = "general c3-3 c4-3 c5-4 c6-6"
  override val linkId = "query-function"

  override def assert(name: String, result: InternalExecutionResult) {
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
      case "foreach" =>
        assertStats(result, nodesCreated = 3, labelsAdded = 3, propertiesSet = 3)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=coll" =>
        Map("coll" -> List(1,2,3))
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("prop" -> "Andrés"),
    "B" -> Map("prop" -> "Tobias"),
    "C" -> Map("prop" -> "Chris"))

  def text = """
###assertion=returns-one parameters=coll
RETURN

length({coll})
###

Length of the collection.

###assertion=returns-one parameters=coll
RETURN

head({coll}), last({coll}), tail({coll})
###

+head+ returns the first, +last+ the last element
of the collection. +tail+ the remainder of the
collection. All return null for an empty collection.

###assertion=returns-one parameters=value
MATCH path=(n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
WITH nodes(path) as coll
RETURN

[x IN coll WHERE x.prop <> {value} | x.prop]
###

Combination of filter and extract in a concise notation.

###assertion=returns-one
MATCH n WHERE id(n) = %A%
WITH [n] as coll
RETURN

extract(x IN coll | x.prop)
###

A collection of the value of the expression for each element in the orignal collection.

###assertion=returns-one parameters=value
MATCH n WHERE id(n) = %A%
WITH [n] as coll
RETURN

filter(x IN coll WHERE x.prop <> {value})
###

A filtered collection of the elements where the predicate is `TRUE`.

###assertion=returns-one
MATCH n WHERE id(n) = %A%
WITH [n] as coll
RETURN

reduce(s = "", x IN coll | s + x.prop)
###

Evaluate expression for each element in the collection, accumulate the results.

###assertion=foreach
WITH ["Alice","Bob","Charlie"] AS coll

FOREACH (value IN coll |
 CREATE (:Person {name:value}))
###

Execute a mutating operation for each element in a collection.
             """
}

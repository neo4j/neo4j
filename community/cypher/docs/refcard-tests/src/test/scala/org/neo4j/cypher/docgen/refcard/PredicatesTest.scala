/**
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
import org.neo4j.cypher.{ ExecutionResult, QueryStatisticsTestSupport }
import org.neo4j.cypher.docgen.RefcardTest

class PredicatesTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A:Person KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "Predicates"
  val css = "general c2-2 c3-3 c4-2 c5-2 c6-4"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "returns-two" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 2)
      case "returns-three" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 3)
      case "returns-four" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 4)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Bob")
      case "parameters=anothername" =>
        Map("value" -> "Andrés")
      case "parameters=regex" =>
        Map("regex" -> "Tob.*")
      case "parameters=names" =>
        Map("value1" -> "Peter", "value2" -> "Tobias")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> "Andrés", "number" -> 5),
    "B" -> Map("property" -> "Tobias"),
    "C" -> Map("property" -> "Chris"))

  def text = """
###assertion=returns-one parameters=aname
START n=node(%A%), m=node(%B%)
MATCH (n)-->(m)
WHERE

n.property <> {value}

RETURN n,m###

Use comparison operators.

###assertion=returns-three
START n=node(*)
WHERE

has(n.property)

RETURN n###

Use functions.

###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH (n)-->(m)
WHERE

n.number >= 1 AND n.number <= 10

RETURN n,m###

Use boolean operators to combine predicates.

###assertion=returns-one
MATCH (n:Person)
WHERE

n:Person

RETURN n###

Check for node labels.

###assertion=returns-one
START n=node(%A%), m=node(%B%)
OPTIONAL MATCH (n)-[identifier]->(m)
WHERE

identifier IS NULL

RETURN n,m###

Check if something is `NULL`.

###assertion=returns-one parameters=aname
START n=node(*)
WHERE

NOT has(n.property) OR n.property = {value}

RETURN n###

Either property does not exist or predicate is +TRUE+.

###assertion=returns-none parameters=aname
START n=node(*)
WHERE

n.property = {value}

RETURN n###

Non-existing property returns `NULL`, which is not equal to anything.

###assertion=returns-one parameters=regex
START n=node(*)
WHERE HAS(n.property) AND

n.property =~ "Tob.*"

RETURN n###

Regular expression.

###assertion=returns-four
START n=node(*), m=node(*)
WHERE

(n)-[:KNOWS]->(m)

RETURN n###

Make sure the pattern has at least one match.

###assertion=returns-none
START n=node(%A%), m=node(%B%)
WHERE

NOT (n)-[:KNOWS]->(m)

RETURN n###

Exclude matches to `(n)-[:KNOWS]->(m)` from the result.

###assertion=returns-one parameters=names
START n=node(*)
WHERE has(n.property) AND

n.property IN [{value1}, {value2}]

RETURN n###

Check if an element exists in a collection.
"""
}

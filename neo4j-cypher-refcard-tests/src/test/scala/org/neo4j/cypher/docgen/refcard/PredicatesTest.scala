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

class PredicatesTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "Predicates"

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
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Bob")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("propertyName" -> 10),
    "B" -> Map("propertyName" -> 20),
    "C" -> Map("value" -> 30))

  def text = """.Predicates
[refcard]
----
###assertion=returns-one parameters=aname
START n=node(%A%), m=node(%B%)
MATCH (n)-->(m)
WHERE

n.propertyName <> {value}

RETURN n,m###

Use comparison operators.

###assertion=returns-one parameters=aname
START n=node(%A%), m=node(%B%)
MATCH (n)-->(m)
WHERE

n.propertyName <> {value} AND m.propertyName <> {value}

RETURN n,m###

Use boolean operators to combine predicates.


###assertion=returns-none
START n=node(%A%), m=node(%B%)
MATCH (n)-[identifier?]->(m)
WHERE

identifier IS NULL

RETURN n,m###

Check if something is `null`.

###assertion=returns-two parameters=aname
START n=node(*)
WHERE

n.propertyName? = {value}

RETURN n###

Defaults to `true` if the property does not exist.

###assertion=returns-none parameters=aname
START n=node(*)
WHERE

n.propertyName! = {value}

RETURN n###

Defaults to `false` if the property does not exist.
----
"""
}

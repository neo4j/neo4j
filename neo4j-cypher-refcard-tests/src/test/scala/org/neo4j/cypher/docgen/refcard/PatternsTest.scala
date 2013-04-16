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

class PatternsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "Patterns"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "related" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "create" =>
        assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
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
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """.Patterns
["refcard", cardcss="general"]
----
###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-->(m)

RETURN n,m###

A relationship from `n` to `m` exists.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)--(m)

RETURN n,m###

A relationship from `n` to `m` or from `m` to `n` exists.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(m)<-[:KNOWS]-(n)

RETURN n,m###

A relationship from `n` to `m` of type `KNOWS` exists.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-[:KNOWS|LOVES]->(m)

RETURN n,m###

A relationship from `n` to `m` of type `KNOWS` or `LOVES` exists.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-[r]->(m)

RETURN r###

Bind an identifier to the relationship.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-[r?]->(m)

RETURN r###

Optional relationship.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-[*1..5]->(m)

RETURN n,m###

Variable length paths.

###assertion=related
START n=node(%A%), m=node(%B%)
MATCH

(n)-[*]->(m)

RETURN n,m###

Any depth.

###assertion=create parameters=aname
START n=node(%A%)
CREATE UNIQUE

(n)-[:KNOWS]->(m {propertyName: {value}})

RETURN m###

Match or set properties in `CREATE` or `CREATE UNIQUE` clauses.
----
"""
}

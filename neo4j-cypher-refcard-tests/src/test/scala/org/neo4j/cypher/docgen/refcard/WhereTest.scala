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

class WhereTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT FRIEND A", "A FRIEND B", "B FRIEND C", "C FRIEND ROOT")
  val section = "refcard"
  val title = "Where"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Bob")
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("propertyName" -> "Andrés"),
    "B" -> Map("propertyName" -> "Tobias"),
    "C" -> Map("propertyName" -> "Chris"))

  def text = """.WHERE
["refcard", cardcss="read c2-2 c3-2"]
----
###assertion=returns-one parameters=aname
START n=node(%A%), m=node(%B%)
MATCH (n)-->(m)

WHERE n.propertyName <> {value}

RETURN n,m###

Use a predicate to filter.
----
"""
}

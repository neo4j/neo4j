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

class CreateUniqueTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val title = "CREATE UNIQUE"
  val css = "col carddeprecation c2-1 c3-3 c4-4 c5-5 c6-6"
  override val linkId = "query-create-unique"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
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

  def text = """
###assertion=create parameters=aname
//
MATCH n WHERE id(n) = %A%

CREATE UNIQUE
    (n)-[:KNOWS]->(m {property: {value}})

RETURN m###

Match pattern or create it if it does not exist.
The pattern can not include any optional parts.

"""
}

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.sunshine.docgen

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node
class MatchTest extends DocumentingTestBase
{
  def indexProps: List[String] = List("name")

  def graphDescription: List[String] = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E")

  def section: String = "MATCH"

  @Test def allRelationships()
  {
    testQuery(
      title = "Related nodes",
      text = "The symbol -- means related to, without regard to type or direction.",
      queryText = """start n=(%A%) match (n)--(x) return x""",
      returns = """All nodes related to A are returned""",
      (p) => assertEquals(List(node("B"), node("D"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships()
  {
    testQuery(
      title = "Outgoing relationships",
      text = "When the direction of a relationship is interesting, it is shown by using --> or <-- ",
      queryText = """start n=(%A%) match (n)-->(x) return x""",
      returns = """All nodes that A has outgoing relationships to.""",
      (p) => assertEquals(List(node("B"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships2()
  {
    testQuery(
      title = "This is the simplest way of finding outgoing relationships.",
      text = "",
      queryText = """start n=(%A%) match (n)-[r]->() return r""",
      returns = """All outgoing relationships from node A.""",
      (p) => assertEquals(2, p.size)
    )
  }

  @Test def relatedNodesByRelationshipType()
  {
    testQuery(
      title = "Match by relationship type.",
      text = "When you know the relationship type you want to match on, you can specify it by using a colon.",
      queryText = """start n=(%A%) match (n)-[:BLOCKS]->(x) return x""",
      returns = """All nodes that are BLOCKed by A.""",
      (p) => assertEquals(List(node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def relationshipsByType()
  {
    testQuery(
      title = "Match by relationship type and use a variable.",
      text = "If you both want to introduce a variable to hold the relationship, and specify the relationship type you want, " +
        "just separate them with a coma.",
      queryText = """start n=(%A%) match (n)-[r, :BLOCKS]->() return r""",
      returns = """All BLOCK relationship going out from A.""",
      (p) => assertEquals(1, p.size)
    )
  }

}
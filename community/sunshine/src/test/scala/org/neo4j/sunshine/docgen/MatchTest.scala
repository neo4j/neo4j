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


class MatchTest extends DocumentingTestBase {
  def indexProps: List[String] = List("name")

  def graphDescription: List[String] = List("A KNOWS B", "A KNOWS C", "D KNOWS A")

  def section: String = "MATCH"

  @Test def allRelationships() {
    testQuery(
      title = "This is the simplest way of finding all related nodes",
      query = """start n=(nodes,name,"A") match (n)--(x) return x""",
      returns = """All nodes related to A are returned""",
      (p) => assertEquals(List(node("B"),node("C"), node("D")), p.columnAs[Node]("x") .toList)
    )
  }

  @Test def allOutgoingRelationships() {
    testQuery(
      title = "This is the simplest way of finding related nodes",
      query = """start n=(nodes,name,"A") match (n)-->(x) return x""",
      returns = """All nodes that A has outgoing relationships to""",
      (p) => assertEquals(List(node("B"),node("C")), p.columnAs[Node]("x") .toList)
    )
  }

}
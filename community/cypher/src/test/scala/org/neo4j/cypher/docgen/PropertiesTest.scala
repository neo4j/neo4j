package org.neo4j.cypher.docgen

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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node

class PropertiesTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "B KNOWS C")

  def section = "Properties"

  @Test def relationship_type() {
    testQuery(
      title = "Relationship type",
      text = """When you want to output the relationship type, and not the whole relationship, you can use the special property TYPE, that gives you a string representation of the relationship type""",
      queryText = """start n=(%A%) match (n)-[r]->() return r.TYPE""",
      returns = """The relationship type of r.""",
      (p) => assertEquals("KNOWS", p.columnAs[String]("r.TYPE").toList.head))
  }

  @Test def path_length() {
    testQuery(
      title = "Path length",
      text = """To return or filter on the length of a path, use the special property LENGTH""",
      queryText = """start a=(%A%) match p=a-->b-->c return p.LENGTH""",
      returns = """The length of the path p.""",
      (p) => assertEquals(5, p.columnAs[Int]("p.LENGTH").toList.head))
  }

  @Test def nodes_in_path() {
    testQuery(
      title = "Nodes in path",
      text = """To return all the nodes in a path, use the special property NODES""",
      queryText = """start a=(%A%) match p=a-->b-->c return p.NODES""",
      returns = """All the nodes in the path p.""",
      (p) => assert(List(node("A"), node("B"), node("C")) === p.columnAs[List[Node]]("p.NODES").toList.head)
    )
  }
}
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
class PropertiesTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "B KNOWS C")

  def section = "Properties"

//  override val properties = Map("A" -> Map("<<!!__??>>" -> "Yes!", "age" -> 55))
  @Test def relationship_type() {
    testQuery(
      title = "Relationship type",
      text = """When you want to output the relationship type, and not the whole relationship, you can use the special property TYPE.
      If you need to use a normal property that is named TYPE, you can use the quote sign, like this: node.`TYPE` """,
      queryText = """start n=(%A%) match (n)-[r]->() return r.TYPE""",
      returns = """The relationship type of r.""",
      (p) => assertEquals("KNOWS", p.columnAs[String]("r~TYPE").toList.head))
  }
}
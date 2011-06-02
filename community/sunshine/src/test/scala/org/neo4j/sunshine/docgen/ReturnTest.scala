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

class ReturnTest extends DocumentingTestBase
{
  def indexProps = List()

  def graphDescription = List("A KNOWS B")

  def properties = Map("A" -> Map("age" -> 35))

  def section = "Return"

  @Test def returnNode()
  {
    testQuery(
      title = "Return nodes",
      text = "To return a node, list it in the return statemenet.",
      queryText = """start n=(%A%) return n""",
      returns = """The node.""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList)
    )
  }

  @Test def returnProperty()
  {
    testQuery(
      title = "Return property",
      text = "To return a property, use the dot separator, like this:",
      queryText = """start n=(%A%) return n.name""",
      returns = """The the value of the property 'name'.""",
      (p) => assertEquals(List(Map("n.name" -> "A")), p.toList)
    )
  }
}
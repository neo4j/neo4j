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

import org.neo4j.graphdb.Node
import scala.collection.JavaConverters._
import org.junit.Assert.assertThat
import org.junit.Assert.assertEquals
import org.junit.matchers.JUnitMatchers.hasItem
import org.junit.Test

class StartTest extends DocumentingTestBase
{
  def graphDescription = List("A KNOWS B", "A KNOWS C")

  def indexProps = List("name")

  def section: String = "Start"

  @Test def nodes_by_id()
  {
    testQuery(
      title = "Including start nodes by id",
      query = "start n=(0) return n",
      returns = "The reference node is returned",
      (p) => assertThat(p.columnAs[Node]("n").toList.asJava, hasItem(db.getReferenceNode))
    )
  }

  @Test def nodes_by_index()
  {
    testQuery(
      title = "Including start nodes by index lookup",
      query = """start n=(nodes,name,"A") return n""",
      returns = """The node indexed with name "A" is returned""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList)
    )
  }
}


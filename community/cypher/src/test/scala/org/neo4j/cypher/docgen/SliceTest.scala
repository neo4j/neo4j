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
package org.neo4j.cypher.docgen

import org.neo4j.graphdb.Node
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

class SliceTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D", "A KNOWS E")

  def indexProps = List()

  def section: String = "Slice"

  @Test def returnFirstThree() {
    testQuery(
      title = "Return first part",
      text = "To return a subset of the result, starting from the top, use this syntax:",
      queryText = "start n=(%A%, %B%, %C%, %D%, %E%) return n slice 3",
      returns = "The top three items are returned",
      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test @Ignore def returnLastThree() {
    testQuery(
      title = "Return last part",
      text = "To return the last items of a result, use slice with a negative number.",
      queryText = "start n=(%A%, %B%, %C%, %D%, %E%) return n slice -3",
      returns = "The reference node is returned",
      (p) => assertEquals(List(node("C"), node("D"), node("E")), p.columnAs[Node]("n").toList))
  }
}


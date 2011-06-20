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
package org.neo4j.cypher.docgen.aggregation

import org.neo4j.cypher.docgen.{AggregationTest, DocumentingTestBase}
import org.junit.Test
import org.junit.Assert._

class SumTest extends DocumentingTestBase with AggregationTest {
  def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D")

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("foo" -> 13),
    "B" -> Map("foo" -> 33),
    "C" -> Map("foo" -> 44)
  )

  def section = "Sum"

  @Test def sumProperty() {
    testQuery(
      title = "Sum properties",
      text = "This is an example of how you can use SUM.",
      queryText = "start n=(%A%,%B%,%C%) return sum(n.foo)",
      returns = "The sum of all the values in the property 'foo'.",
      (p) => assertEquals(Map("sum(n.foo)" -> (13+33+44)), p.toList.head))
  }
}
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

class AggregationTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D")


  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> 13),
    "B" -> Map("property" -> 33),
    "C" -> Map("property" -> 44)
  )

  def section = "Aggregation"

  @Test def countNodes() {
    testQuery(
      title = "Count nodes",
      text = "To count the number of nodes, for example the number of nodes connected to one node, you can use +count(*)+.",
      queryText = "start n=(%A%) match (n)-->(x) return n, count(*)",
      returns = "The start node and the count of related nodes.",
      (p) => assertEquals(Map("n" -> node("A"), "count(*)" -> 3), p.toList.head))
  }
  
  @Test def countRelationshipsByType() {
    testQuery(
      title = "Group Count Relationship Types",
      text = "To count the groups of relationship types, return the types and count them with +count(*)+.",
      queryText = "start n=(%A%) match (n)-[r]->() return type(r), count(*)",
      returns = "The relationship types and their group count.",
      (p) => assertEquals(Map("TYPE(r)" -> "KNOWS", "count(*)" -> 3), p.toList.head))
  }

  @Test def countEntities() {
    testQuery(
      title = "Count entities",
      text = "Instead of counting the number of results with count(*), it might be more expressive to include " +
        "the name of the identifier you care about.",
      queryText = "start n=(%A%) match (n)-->(x) return count(x)",
      returns = "The number of connected nodes from the start node.",
      (p) => assertEquals(Map("count(x)" -> 3), p.toList.head))
  }

  @Test def countNonNullValues() {
    testQuery(
      title = "Count non null values",
      text = "You can count the non-null values by using +count(<identifier>)+.",
      queryText = "start n=(%A%,%B%,%C%,%D%) return count(n.property?)",
      returns = "The count of related nodes.",
      (p) => assertEquals(Map("count(n.property)" -> 3), p.toList.head))
  }

  @Test def sumProperty() {
    testQuery(
      title = "SUM",
      text = "The +SUM+ aggregation function simply sums all the numeric values it encounters. " +
        "Null values are silently dropped. This is an example of how you can use +SUM+.",
      queryText = "start n=(%A%,%B%,%C%) return sum(n.property)",
      returns = "The sum of all the values in the property 'property'.",
      (p) => assertEquals(Map("sum(n.property)" -> (13 + 33 + 44)), p.toList.head))
  }

  @Test def avg() {
    testQuery(
      title = "AVG",
      text = "+AVG+ calculates the average of a numeric column.",
      queryText = "start n=(%A%,%B%,%C%) return avg(n.property)",
      returns = "The average of all the values in the property 'property'.",
      (p) => assertEquals(Map("avg(n.property)" -> 30), p.toList.head))
  }

  @Test def min() {
    testQuery(
      title = "MIN",
      text = "+MIN+ takes a numeric property as input, and returns the smallest value in that column.",
      queryText = "start n=(%A%,%B%,%C%) return min(n.property)",
      returns = "The smallest of all the values in the property 'property'.",
      (p) => assertEquals(Map("min(n.property)" -> 13), p.toList.head))
  }

  @Test def max() {
    testQuery(
      title = "MAX",
      text = "+MAX+ find the largets value in a numeric column.",
      queryText = "start n=(%A%,%B%,%C%) return max(n.property)",
      returns = "The largest of all the values in the property 'property'.",
      (p) => assertEquals(Map("max(n.property)" -> 44), p.toList.head))
  }
}
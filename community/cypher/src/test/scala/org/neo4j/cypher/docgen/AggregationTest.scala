package org.neo4j.cypher.docgen

import org.junit.Test
import org.junit.Assert._
class AggregationTest extends DocumentingTestBase {
  def indexProps = List()

  def graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D")

  def section = "Aggregation"

  @Test def returnFirstThree() {
    testQuery(
      title = "Count nodes",
      text = "To count the number of nodes, for example the number of nodes connected to one node, you can use count(*).",
      queryText = "start n=(%A%) match (n)-->(x) return n, count(*)",
      returns = "The start node and the count of related nodes.",
      (p) => assertEquals(Map("n" -> node("A"), "count(*)" -> 3), p.toList.head))
  }
}
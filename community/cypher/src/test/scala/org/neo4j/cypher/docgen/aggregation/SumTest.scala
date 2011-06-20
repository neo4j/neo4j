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
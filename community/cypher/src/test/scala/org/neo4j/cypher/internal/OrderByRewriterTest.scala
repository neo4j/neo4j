package org.neo4j.cypher.internal

import commands._
import org.scalatest.Assertions
import org.junit.Test

class OrderByRewriterTest extends Assertions {

  @Test
  def rewriteOrderBy() {
    // start a=node(0) return a, count(*) order by COUNT(*)

    val q = Query.
      start(NodeById("a", 1)).
      aggregation(CountStar()).
      columns("count(*)").
      orderBy(SortItem(CountStar("apa"), true)).
      returns()

    val expected = Query.
      start(NodeById("a", 1)).
      aggregation(CountStar()).
      columns("count(*)").
      orderBy(SortItem(CountStar(), true)).
      returns()

    assert(OrderByRewriter(q) === expected)

  }
}
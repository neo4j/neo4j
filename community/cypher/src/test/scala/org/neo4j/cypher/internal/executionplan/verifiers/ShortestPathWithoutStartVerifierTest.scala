package org.neo4j.cypher.internal.executionplan.verifiers

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.{ShortestPath, NodeById, RelatedTo, Query}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.PatternException


class ShortestPathWithoutStartVerifierTest  extends Assertions {
  @Test
  def should_throw_on_shortestpath_without_start() {
    //GIVEN
    val q = Query.
      matches(ShortestPath("p", "a", "b", Seq(), Direction.INCOMING, Some(1), false, true, None)).
      returns()

    //WHEN & THEN
    val e = intercept[PatternException](ShortestPathWithoutStartVerifier.verify(q))
    assert(e.getMessage === "Can't use shortest path without explicit START clause.")
  }

  @Test
  def should_throw_on_shortestpath_without_start2() {
    //GIVEN
    val q = Query.
      matches(
      RelatedTo("a2", "b2", "r2", Nil, Direction.OUTGOING, false),
      ShortestPath("p", "a", "b", Seq(), Direction.INCOMING, Some(1), false, true, None)).
      returns()

    //WHEN & THEN
    val e = intercept[PatternException](ShortestPathWithoutStartVerifier.verify(q))
    assert(e.getMessage === "Can't use shortest path without explicit START clause.")
  }

  @Test
  def should_not_throw_on_patterns_with_start() {
    //GIVEN
    val q = Query.
      start(NodeById("a", 0)).
      matches(
      RelatedTo("a2", "b2", "r2", Nil, Direction.OUTGOING, false),
      ShortestPath("p", "a", "b", Seq(), Direction.INCOMING, Some(1), false, true, None)
    ).
      returns()

    //WHEN & THEN doesn't throw exception
    ShortestPathWithoutStartVerifier.verify(q)
  }

  @Test
  def should_not_throw_on_patterns_with_no_shortest_path() {
    //GIVEN
    val q = Query.
      matches(
      RelatedTo("a", "b", "r", Nil, Direction.OUTGOING, false),
      RelatedTo("a2", "b2", "r2", Nil, Direction.OUTGOING, false)
    ).
      returns()

    //WHEN & THEN doesn't throw exception
    ShortestPathWithoutStartVerifier.verify(q)
  }

}
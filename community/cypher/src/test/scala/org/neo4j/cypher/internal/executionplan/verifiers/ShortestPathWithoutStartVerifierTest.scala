/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.verifiers

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.{ShortestPath, NodeById, RelatedTo, Query}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.PatternException
import java.nio.ByteBuffer


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
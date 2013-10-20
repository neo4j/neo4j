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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.verifiers

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.{SingleNode, NodeById, Query, RelatedTo}
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.cypher.PatternException


class OptionalPatternWithoutStartVerifierTest extends Assertions {
  @Test
  def should_throw_on_optional_pattern_without_start() {
    //GIVEN
    val q = Query.
      matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Nil, Direction.OUTGOING, true)).
      returns()

    //WHEN & THEN
    val e = intercept[PatternException](OptionalPatternWithoutStartVerifier.verify(q))
    assert(e.getMessage === "Can't use optional patterns without explicit START clause. Optional relationships: `r`")
  }

  @Test
  def should_throw_on_optional_pattern_without_start2() {
    //GIVEN
    val q = Query.
      matches(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r", Nil, Direction.OUTGOING, true),
      RelatedTo(SingleNode("a2"), SingleNode("b2"), "r2", Nil, Direction.OUTGOING, true)).
      returns()

    //WHEN & THEN
    val e = intercept[PatternException](OptionalPatternWithoutStartVerifier.verify(q))
    assert(e.getMessage === "Can't use optional patterns without explicit START clause. Optional relationships: `r`, `r2`")
  }

  @Test
  def should_not_throw_on_patterns_with_start() {
    //GIVEN
    val q = Query.
      start(NodeById("a", 0)).
      matches(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r", Nil, Direction.OUTGOING, true),
      RelatedTo(SingleNode("a2"), SingleNode("b2"), "r2", Nil, Direction.OUTGOING, true)
    ).
      returns()

    //WHEN & THEN doesn't throw exception
    OptionalPatternWithoutStartVerifier.verify(q)
  }
}
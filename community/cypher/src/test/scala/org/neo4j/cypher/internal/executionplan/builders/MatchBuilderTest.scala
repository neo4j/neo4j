/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.pipes.FakePipe
import org.junit.Assert._
import org.neo4j.cypher.internal.executionplan.{Solved, Unsolved, PartiallySolvedQuery}
import org.neo4j.cypher.internal.symbols.{NodeType, MapType, SymbolTable, Identifier}
import org.neo4j.cypher.internal.commands.{ShortestPath, NodeById, True, RelatedTo}


class MatchBuilderTest extends Assertions with PipeBuilder {

  val builder = new MatchBuilder

  @Test
  def should_take_on_satisfied_match() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))

    val p = createPipe(nodes = Seq("l"))

    assertTrue(builder.isDefinedAt(p, q))
  }

  @Test
  def should_not_accept_work_until_all_start_points_are_found() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0)), Unsolved(NodeById("r", 1))),
      patterns = Seq(Unsolved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))

    val p = createPipe(nodes = Seq("l"))

    assertFalse("Should not accept this query", builder.isDefinedAt(p, q))
  }

  @Test
  def should_solve_fixed_parts_of_the_pattern() {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))

    val inP = createPipe(nodes = Seq("l"))
    
    val (_, q) = builder(inP, inQ)

    assert(q.patterns === Seq(Solved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))
  }

  @Test
  def should_solve_part_of_the_pattern_eagerly() {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Unsolved(NodeById("b", 1))),
      patterns = Seq(Unsolved(RelatedTo("a", "r", "rel", None, Direction.OUTGOING, false, True())),
        Unsolved(RelatedTo("b", "r2", "rel2", None, Direction.OUTGOING, false, True()))))

    val inP  = createPipe(nodes = Seq("a"))
    
    val (_, q) = builder(inP, inQ)

    assert(q.patterns.toSet === Set(Solved(RelatedTo("a", "r", "rel", None, Direction.OUTGOING, false, True())),
      Unsolved(RelatedTo("b", "r2", "rel2", None, Direction.OUTGOING, false, True()))))
  }

  @Test
  def should_solve_multiple_patterns_at_once_if_possible() {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 1))),
      patterns = Seq(Unsolved(RelatedTo("a", "r", "rel", None, Direction.OUTGOING, false, True())),
        Unsolved(RelatedTo("b", "r2", "rel2", None, Direction.OUTGOING, false, True()))))

    val inP = createPipe(nodes = Seq("a","b"))

    val (_, q) = builder(inP, inQ)

    assert(q.patterns.toSet === Set(Solved(RelatedTo("a", "r", "rel", None, Direction.OUTGOING, false, True())),
      Solved(RelatedTo("b", "r2", "rel2", None, Direction.OUTGOING, false, True()))))
  }

  @Test
  def should_not_accept_patterns_with_only_shortest_path() {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 0))),
      patterns = Seq(Unsolved(ShortestPath("p", "a", "b", None, Direction.OUTGOING, None, false, true, None))))

    val inP = createPipe(nodes = Seq("l"))

    assertFalse(builder.isDefinedAt(inP,inQ))
  }
}
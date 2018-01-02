/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.{NodeById, RelatedTo, ShortestPath, SingleNode}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_3.pipes.MatchPipe
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.OUTGOING

class MatchBuilderTest extends BuilderTest {

  val builder = new MatchBuilder

  test("should_take_on_satisfied_match") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty))))

    val p = createPipe(nodes = Seq("l"))

    assertAccepts(p, q)
  }

  test("should_not_accept_work_until_all_start_points_are_found") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0)), Unsolved(NodeById("r", 1))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty))))

    val p = createPipe(nodes = Seq("l"))

    assertRejects(p, q)
  }

  test("should_solve_fixed_parts_of_the_pattern") {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty))))

    val inP = createPipe(nodes = Seq("l"))

    val q = assertAccepts(inP, inQ).query

    q.patterns should equal(Seq(Solved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty))))
  }

  test("should_solve_part_of_the_pattern_eagerly") {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Unsolved(NodeById("b", 1))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("a"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty)),
        Unsolved(RelatedTo(SingleNode("b"), SingleNode("r2"), "rel2", Seq(), OUTGOING, Map.empty))))

    val inP = createPipe(nodes = Seq("a"))

    val q = assertAccepts(inP, inQ).query

    q.patterns.toSet should equal(Set(
      Solved(RelatedTo(SingleNode("a"), SingleNode("r"), "rel", Seq(), OUTGOING, Map.empty)),
      Unsolved(RelatedTo(SingleNode("b"), SingleNode("r2"), "rel2", Seq(), OUTGOING, Map.empty))
    ))
  }

  test("should_solve_multiple_patterns_at_once_if_possible") {
    val r1 = RelatedTo("a", "b", "r1", Seq(), OUTGOING)
    val r2 = RelatedTo("b", "c", "r2", Seq(), OUTGOING)
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 1))),

      patterns = Seq(
        Unsolved(r1),
        Unsolved(r2)))

    val inP = createPipe(nodes = Seq("a", "b"))

    val q = assertAccepts(inP, inQ).query

    q.patterns.toSet should equal(Set(Solved(r1), Solved(r2)))
  }

  test("should_not_accept_patterns_with_only_shortest_path") {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 0))),
      patterns = Seq(Unsolved(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), OUTGOING, false, None, single = true, None))))

    val inP = createPipe(nodes = Seq("l"))

    assertRejects(inP, inQ)
  }

  test("should_accept_non_optional_parts_of_the_query_first") {
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 0))),
      patterns = Seq(Unsolved(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), OUTGOING, false, None, single = true, None))))

    val inP = createPipe(nodes = Seq("l"))

    assertRejects(inP, inQ)
  }

  test("should_pass_on_the_whole_list_of_identifier_in_match_to_created_pipe") {
    // GIVEN MATCH a-[r1]->b-[r2]->c
    // a-[r1]->b is already solved
    val inQ = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 0))),
      patterns = Seq(
        Solved(RelatedTo("a", "b", "r1", Seq.empty, OUTGOING)),
        Unsolved(RelatedTo("b", "c", "r2", Seq.empty, OUTGOING))
      ))

    val inP = createPipe(nodes = Seq("a", "r1", "b"))

    val result = assertAccepts(inP, inQ)

    val matchPipe = result.pipe.asInstanceOf[MatchPipe]
    matchPipe.identifiersInClause should equal(Set("a", "r1", "b", "r2", "c"))
  }
}

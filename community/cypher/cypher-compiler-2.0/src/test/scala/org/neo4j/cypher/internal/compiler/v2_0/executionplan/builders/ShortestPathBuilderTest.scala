/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_0.commands._

class ShortestPathBuilderTest extends BuilderTest {

  val builder = new ShortestPathBuilder

  @Test
  def should_not_accept_if_no_shortest_paths_exist() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), Direction.OUTGOING, Map.empty))))

    val p = createPipe(nodes = Seq("l"))

    assertRejects(p, q)
  }

  @Test
  def should_accept_if_both_start_and_end_have_been_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("a", 0)), Solved(NodeById("b", 0))),
      patterns = Seq(Unsolved(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.OUTGOING, None, single = true, None))))

    val p = createPipe(nodes = Seq("a", "b"))

    val resultQ = assertAccepts(p, q).query

    assert(resultQ.patterns == Seq(Solved(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.OUTGOING, None, single = true, None))))
  }
}

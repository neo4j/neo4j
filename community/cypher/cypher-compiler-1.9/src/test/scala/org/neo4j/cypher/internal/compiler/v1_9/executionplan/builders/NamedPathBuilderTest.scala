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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.neo4j.graphdb.Direction
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v1_9.commands.{NamedPath, NodeById, RelatedTo, True}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PartiallySolvedQuery


class NamedPathBuilderTest extends BuilderTest {
  val builder = new NamedPathBuilder


  @Test
  def should_not_accept_if_pattern_is_not_yet_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True()))),
      namedPaths = Seq(Unsolved(NamedPath("p", RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l"))

    assertFalse("Builder should not accept this", builder.canWorkWith(plan(p, q)))
  }

  @Test
  def should_accept_if_pattern_is_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Solved(RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True()))),
      namedPaths = Seq(Unsolved(NamedPath("p", RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))

    assertTrue("Builder should not accept this", builder.canWorkWith(plan(p, q)))

    val resultPlan = builder(plan(p, q))

    assert(resultPlan.query.namedPaths == Seq(Solved(NamedPath("p", RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True())))))
  }

  @Test
  def should_not_accept_unless_all_parts_of_the_named_path_are_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(
        Solved(RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True())),
        Unsolved(RelatedTo("r", "x", "rel2", Seq(), Direction.OUTGOING, false, True()))
      ),
      namedPaths = Seq(Unsolved(NamedPath("p",
        RelatedTo("l", "r", "rel", Seq(), Direction.OUTGOING, false, True()),
        RelatedTo("r", "x", "rel2", Seq(), Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))

    assertFalse("Builder should not accept this", builder.canWorkWith(plan(p, q)))
  }
}

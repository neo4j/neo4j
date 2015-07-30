/*
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.{NamedPath, NodeById, RelatedTo, SingleNode}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_2.pipes.NamedPathPipe
import org.neo4j.graphdb.Direction

class NamedPathBuilderTest extends BuilderTest {

  val builder = new NamedPathBuilder

  test("should_not_accept_if_pattern_is_not_yet_solved") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), Direction.OUTGOING, Map.empty))),
      namedPaths = Seq(Unsolved(NamedPath("p", ParsedRelation("rel", "l", "r", Seq(), Direction.OUTGOING))))
    )

    val p = createPipe(nodes = Seq("l"))

    assertRejects(p, q)
  }

  test("should_accept_if_pattern_is_solved") {
    val namedPath = NamedPath("p", ParsedRelation("rel", "l", "r", Seq(), Direction.OUTGOING))
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Solved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), Direction.OUTGOING, Map.empty))),
      namedPaths = Seq(Unsolved(namedPath))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))

    val result = assertAccepts(p, q)

    result.query.namedPaths should equal(Seq(Solved(namedPath)))
    result.pipe should equal(NamedPathPipe(p, "p", namedPath.pathPattern))
  }

  test("should_not_accept_unless_all_parts_of_the_named_path_are_solved") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(
        Solved(RelatedTo(SingleNode("l"), SingleNode("r"), "rel", Seq(), Direction.OUTGOING, Map.empty)),
        Unsolved(RelatedTo(SingleNode("r"), SingleNode("x"), "rel2", Seq(), Direction.OUTGOING, Map.empty))
      ),
      namedPaths = Seq(Unsolved(NamedPath("p",
        ParsedRelation("rel", "l", "r", Seq(), Direction.OUTGOING),
        ParsedRelation("rel2", "r", "x", Seq(), Direction.OUTGOING))))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))
    assertRejects(p, q)
  }
}

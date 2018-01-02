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

import org.neo4j.cypher.internal.compiler.v2_3.commands.{RelatedTo, AllNodes}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{MergePatternAction}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.MergeIntoPipe
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection

class MergeIntoBuilderTest extends BuilderTest {
  override def builder = new MergeIntoBuilder
  context = mock[PlanContext]

  test("should not take on a plan without MERGE between two nodes") {
    val q = newQuery(start = Seq(AllNodes("x")))

    assertRejects(q)
  }

  test("should not take on a plan MERGE between two nodes when the nodes have not yet been bound") {

    val r = RelatedTo("a", "b", "r", "T", SemanticDirection.OUTGOING)
    val mergeP = MergePatternAction(patterns = Seq(r), actions = Seq.empty, Seq.empty, Seq.empty)

    val q = newQuery(
      start = Seq(AllNodes("a"), AllNodes("b")),
      updates = Seq(mergeP)
    )

    val p = createPipe(nodes = Seq("a"))

    assertRejects(p, q)
  }

  test("should take on a plan MERGE between two nodes when the nodes have not yet been bound") {

    val r = RelatedTo("a", "b", "r", "T", SemanticDirection.OUTGOING)
    val mergeP = MergePatternAction(patterns = Seq(r), actions = Seq.empty, Seq.empty, Seq.empty)

    val q = newQuery(
      start = Seq(AllNodes("a"), AllNodes("b")),
      updates = Seq(mergeP)
    )

    val p = createPipe(nodes = Seq("a", "b"))

    val result = assertAccepts(p, q).pipe

    result should equal (MergeIntoPipe(p, "a", "r", "b", SemanticDirection.OUTGOING, "T", Map.empty, Seq.empty, Seq.empty)())
  }

}

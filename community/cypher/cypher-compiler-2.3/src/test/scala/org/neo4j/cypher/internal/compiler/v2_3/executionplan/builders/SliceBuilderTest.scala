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

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.commands.{Slice, SortItem}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery

class SliceBuilderTest extends BuilderTest {

  val builder = new SliceBuilder

  test("should_accept_if_all_work_is_done_and_sorting_not_yet") {
    val q = PartiallySolvedQuery().copy(
      slice = Some(Unsolved(Slice(Some(Literal(10)), Some(Literal(10))))),
      extracted = true
    )

    val p = createPipe(nodes = Seq("x"))

    val resultPlan = assertAccepts(p, q)

    resultPlan.query.slice should equal(q.slice.map(_.solve))
  }

  test("should_not_accept_if_not_yet_sorted") {
    val q = PartiallySolvedQuery().copy(
      slice = Some(Unsolved(Slice(Some(Literal(10)), Some(Literal(10))))),
      extracted = true,
      sort = Seq(Unsolved(SortItem(Literal(1), true)))
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }

  test("should_not_accept_if_no_slice_in_the_query") {
    val q = PartiallySolvedQuery().copy(
      extracted = true
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }
}

/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_0.commands.{Slice, ReturnItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Identifier, Literal}

class ColumnFilterBuilderTest extends BuilderTest {

  val builder = new ColumnFilterBuilder

  @Test def should_accept_if_all_work_is_done_and_sorting_not_yet() {
    val q = PartiallySolvedQuery().copy(
      extracted = true,
      returns = Seq(Solved(ReturnItem(Literal("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("x"))



    val resultQ = assertAccepts(p, q).query

    assert(resultQ.returns === q.returns.map(_.solve))
  }

  @Test def should_not_accept_if_not_yet_extracted() {
    val q = PartiallySolvedQuery().copy(
      extracted = false,
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }

  @Test def should_not_accept_if_not_sorted() {
    val q = PartiallySolvedQuery().copy(
      extracted = true,
      sort = Seq(Unsolved(SortItem(Literal(1), true))),
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }

  @Test def should_not_accept_if_not_sliced() {
    val q = PartiallySolvedQuery().copy(
      extracted = true,
      slice = Some(Unsolved(Slice(Some(Literal(19)), None))),
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }

  @Test def should_not_introduce_column_filter_pipe_unless_needed() {
    val q = PartiallySolvedQuery().copy(
      extracted = true,
      returns = Seq(Solved(ReturnItem(Identifier("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("foo"))

    assertRejects(p, q)
  }

}
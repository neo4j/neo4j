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

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.SortItem
import commands.expressions._
import commands.values.TokenType._
import executionplan.PartiallySolvedQuery
import symbols._
import org.junit.Test

class SortBuilderTest extends BuilderTest {

  val builder = new SortBuilder

  @Test def should_accept_if_all_work_is_done_and_sorting_not_yet() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property(Identifier("x"), PropertyKey("foo")), ascending = true))),
      extracted = true
    )

    val expected = List(Solved(SortItem(CachedExpression("x.foo", CTAny), ascending = true)))

    val p = createPipe(nodes = Seq("x"))

    val resultQ = assertAccepts(p, q).query

    resultQ.sort match {
      case List(Solved(SortItem(CachedExpression(_, _: AnyType), true))) => //correct, don't check anything else
      case _                                                            => assert(resultQ.sort === expected)
    }
  }

  @Test def should_not_accept_if_not_yet_extracted() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property(Identifier("x"), PropertyKey("foo")), ascending = true))),
      extracted = false
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }

  @Test def should_not_accept_aggregations_not_in_return() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(CountStar(), ascending = true))),
      extracted = true
    )

    val p = createPipe(nodes = Seq("x"))

    assertRejects(p, q)
  }
}

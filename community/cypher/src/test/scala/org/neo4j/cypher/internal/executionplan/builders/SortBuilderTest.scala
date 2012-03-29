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

import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.{CachedExpression, Property, SortItem}
import org.neo4j.cypher.internal.symbols.{ScalarType, Identifier}
import org.neo4j.cypher.internal.executionplan.{Solved, Unsolved, PartiallySolvedQuery}

class SortBuilderTest extends PipeBuilder with Assertions {

  val builder = new SortBuilder

  @Test def should_accept_if_all_work_is_done_and_sorting_not_yet() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property("x", "foo"), true))),
      extracted = true
    )

    val expected = List(Solved(SortItem(CachedExpression("x.foo", Identifier("x.foo", ScalarType())), true)))

    val p = createPipe(nodes = Seq("x"))

    assertTrue("Builder should accept this", builder.isDefinedAt(p, q))

    val (_, resultQ) = builder(p, q)

    assert(resultQ.sort === expected)
  }

  @Test def should_not_accept_if_not_yet_extracted() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property("x", "foo"), true))),
      extracted = false
    )

    val p = createPipe(nodes = Seq("x"))

    assertFalse("Builder should accept this", builder.isDefinedAt(p, q))
  }

}
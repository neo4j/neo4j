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
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.executionplan.Unsolved
import org.junit.Assert._
import org.neo4j.cypher.internal.commands._

class AggregationBuilderTest extends PipeBuilder with Assertions {

  val builder = new AggregationBuilder

  @Test def should_accept_if_stuff_looks_ok() {
    val q = PartiallySolvedQuery().
      copy(
      aggregation = Seq(Unsolved(CountStar())),
      returns = Seq(Unsolved(ReturnItem(Entity("n"), "n"))),
      aggregateQuery = Unsolved(true)
    )

    val p = createPipe(nodes = Seq("n"))

    assertTrue("Builder should accept this", builder.isDefinedAt(p, q))

    val (_, resultQ) = builder(p, q)

    val expectedQuery = q.copy(
      aggregation = q.aggregation.map(_.solve),
      aggregateQuery = q.aggregateQuery.solve,
      extracted = true
    )

    assert(resultQ == expectedQuery)
  }

  @Test def should_not_accept_if_there_are_still_other_things_to_do_in_the_query() {
    val q = PartiallySolvedQuery().
      copy(
      start = Seq(Unsolved(NodeById("n", 0))),
      aggregation = Seq(Unsolved(CountStar())),
      returns = Seq(Unsolved(ReturnItem(Entity("n"), "n")))
    )


    val p = createPipe(nodes = Seq())

    assertFalse("Builder should not accept this", builder.isDefinedAt(p, q))
  }
}
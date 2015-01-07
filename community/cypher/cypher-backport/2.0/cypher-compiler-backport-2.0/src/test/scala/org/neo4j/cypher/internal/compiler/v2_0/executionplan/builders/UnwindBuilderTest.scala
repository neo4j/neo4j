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
import org.neo4j.cypher.internal.compiler.v2_0.commands.{Query, ReturnItem, Unwind, AllIdentifiers}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Identifier, Literal, Collection}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.UnwindPipe

class UnwindBuilderTest extends BuilderTest {
  val builder = new UnwindBuilder

  @Test def should_accept_queries_containing_unsolved_load_csv_items() {
    val unwind = Unwind(Collection(Literal(1),Literal(2)),"y")
    val q = Query.
      start(unwind).
      returns(ReturnItem(Identifier("y"),"y",false))

    val result = assertAccepts(q)

    assert(result.query.start === Seq(Solved(unwind)))
    assert(result.pipe.isInstanceOf[UnwindPipe])
  }

  @Test def should_reject_queries_containing_no_unsolved_load_csv_items() {
    val q = Query.start().returns(AllIdentifiers())
    assertRejects(q)
  }
}

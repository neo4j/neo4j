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
import org.neo4j.cypher.internal.compiler.v2_3.commands.{AllIdentifiers, LoadCSV, Query}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LoadCSVPipe
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class LoadCSVBuilderTest extends BuilderTest {
  context = mock[PlanContext]
  val builder = new LoadCSVBuilder

  test("should_accept_queries_containing_unsolved_load_csv_items") {
    val loadCSV = LoadCSV(withHeaders = false, new Literal("file:///tmp/data.csv"), "row", None)
    val q = Query.
      start(loadCSV).
      returns(AllIdentifiers())

    val result = assertAccepts(q)

    result.query.start should equal(Seq(Solved(loadCSV)))
    result.pipe shouldBe a [LoadCSVPipe]
  }

  test("should_reject_queries_containing_no_unsolved_load_csv_items") {
    val q = Query.start().returns(AllIdentifiers())
    assertRejects(q)
  }
}

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
package org.neo4j.cypher.internal.compiler.v1_9.profiler

import org.neo4j.cypher.internal.compiler.v1_9.pipes.{NullPipe, QueryState, Pipe, PipeWithSource}
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v1_9.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanDescription


class ProfilerTest extends Assertions with MockitoSugar {
  @Test
  def should_report_simplest_case() {
    //GIVEN
    val start = NullPipe
    val pipe = new ProfilerPipe(start, "foo", rows = 10, dbAccess = 20)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryState(queryContext, Map.empty, profiler, None)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe.executionPlanDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", rows = 10, dbAccess = 20)
  }

  @Test
  def should_report_multiple_pipes_case() {
    //GIVEN
    val start = NullPipe
    val pipe1 = new ProfilerPipe(start, "foo", rows = 10, dbAccess = 25)
    val pipe2 = new ProfilerPipe(pipe1, "bar", rows = 20, dbAccess = 40)
    val pipe3 = new ProfilerPipe(pipe2, "baz", rows = 1, dbAccess = 2)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryState(queryContext, Map.empty, profiler, None)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe3.executionPlanDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", rows = 10, dbAccess = 25)
    assertRecorded(decoratedResult, "bar", rows = 20, dbAccess = 40)
    assertRecorded(decoratedResult, "baz", rows = 1, dbAccess = 2)
  }

  private def assertRecorded(result: PlanDescription, name: String, rows: Int, dbAccess: Int) {
    val pipeArgs = result.find(name).get.args.toMap
    val recordedHits = pipeArgs("_db_hits")
    val recordedRows = pipeArgs("_rows")

    assert(recordedHits.v === dbAccess)
    assert(recordedRows.v === rows)
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

class ProfilerPipe(source: Pipe, name: String, rows: Int, dbAccess: Int) extends PipeWithSource(source) {
  def executionPlanDescription: PlanDescription = source.executionPlanDescription.andThen(this, name)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    (0 until dbAccess).foreach(x => state.query.createNode())
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }

  def symbols: SymbolTable = SymbolTable()

  def throwIfSymbolsMissing(symbols: SymbolTable) {}
}

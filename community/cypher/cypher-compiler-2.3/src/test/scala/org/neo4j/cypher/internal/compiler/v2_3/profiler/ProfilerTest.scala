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
package org.neo4j.cypher.internal.compiler.v2_3.profiler

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{NestedPipeExpression, ProjectedPath}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{WritesAnyNode, Effects, WritesNodes}
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Argument, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.collection.immutable.::

class ProfilerTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("should report simplest case") {
    //GIVEN
    val start = SingleRowPipe()
    val pipe = new ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe.planDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 20)
  }

  test("should report multiple pipes case") {
    //GIVEN
    val start = SingleRowPipe()
    val pipe1 = new ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25)
    val pipe2 = new ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40)
    val pipe3 = new ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe3.planDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 25)
    assertRecorded(decoratedResult, "bar", expectedRows = 20, expectedDbHits = 40)
    assertRecorded(decoratedResult, "baz", expectedRows = 1, expectedDbHits = 2)
  }

  test("should ignore null pipe in profile") {
    // GIVEN
    val pipes = UnionPipe(List(SingleRowPipe(), SingleRowPipe()), List())
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    // THEN it should not throw an assertion about profiling the same pipe twice.
    materialize(pipes.createResults(queryState))
  }

  test("should count stuff going through Apply multiple times") {
    // GIVEN
    val lhs = new ProfilerTestPipe(SingleRowPipe(), "lhs", rows = 10, dbAccess = 10)
    val rhs = new ProfilerTestPipe(SingleRowPipe(), "rhs", rows = 20, dbAccess = 30)
    val apply = new ApplyPipe(lhs, rhs)()
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(apply.createResults(queryState))
    val decoratedResult = profiler.decorate(apply.planDescription, isProfileReady = true)

    // THEN
    assertRecorded(decoratedResult, "rhs", expectedRows = 10*20, expectedDbHits = 10*30)
  }

  test("count dbhits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val innerPipe = NestedPipeExpression(new ProfilerTestPipe(SingleRowPipe(), "nested pipe", rows = 10, dbAccess = DB_HITS), projectedPath)
    val pipeUnderInspection = ProjectionPipe(SingleRowPipe(), Map("x" -> innerPipe))()

    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val description = pipeUnderInspection.planDescription
    val decoratedResult = profiler.decorate(description, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS)
  }

  test("count dbhits for deeply nested NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val nestedExpression = NestedPipeExpression(new ProfilerTestPipe(SingleRowPipe(), "nested pipe1", rows = 10, dbAccess = DB_HITS), projectedPath)
    val innerInnerPipe = ProjectionPipe(SingleRowPipe(), Map("y"->nestedExpression))()
    val innerPipe = NestedPipeExpression(new ProfilerTestPipe(innerInnerPipe, "nested pipe2", rows = 10, dbAccess = DB_HITS), projectedPath)
    val pipeUnderInspection = ProjectionPipe(SingleRowPipe(), Map("x" -> innerPipe))()

    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val description = pipeUnderInspection.planDescription
    val decoratedResult = profiler.decorate(description, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS * 2)
  }

  test("should not count rows multiple times when the same pipe is used multiple times") {
    val profiler = new Profiler

    val pipe1 = SingleRowPipe()
    val iter1 = Iterator(ExecutionContext.empty, ExecutionContext.empty, ExecutionContext.empty)

    val profiled1 = profiler.decorate(pipe1, iter1)
    profiled1.toList // consume it
    profiled1.asInstanceOf[ProfilingIterator].count should equal(3)

    val pipe2 = SingleRowPipe()
    val iter2 = Iterator(ExecutionContext.empty, ExecutionContext.empty)

    val profiled2 = profiler.decorate(pipe2, iter2)
    profiled2.toList // consume it
    profiled2.asInstanceOf[ProfilingIterator].count should equal(2)
  }

  test("should not count dbhits multiple times when the same pipe is used multiple times") {
    val profiler = new Profiler

    val pipe1 = SingleRowPipe()
    val ctx1 = mock[QueryContext]
    val state1 = new QueryState(ctx1, mock[ExternalResource], Map.empty, mock[PipeDecorator])

    val profiled1 = profiler.decorate(pipe1, state1)
    profiled1.query.createNode()
    profiled1.query.asInstanceOf[ProfilingQueryContext].count should equal(1)

    val pipe2 = SingleRowPipe()
    val ctx2 = mock[QueryContext]
    val state2 = new QueryState(ctx2, mock[ExternalResource], Map.empty, mock[PipeDecorator])

    val profiled2 = profiler.decorate(pipe2, state2)
    profiled2.query.createNode()
    profiled2.query.asInstanceOf[ProfilingQueryContext].count should equal(1)
  }

  private def assertRecorded(result: InternalPlanDescription, name: String, expectedRows: Int, expectedDbHits: Int) {
    val pipeArgs: Seq[Argument] = result.find(name).flatMap(_.arguments)
    pipeArgs shouldNot be(empty)
    pipeArgs.collect {
      case DbHits(count) => withClue("DbHits:")(count should equal(expectedDbHits))
    }

    pipeArgs.collect {
      case Rows(seenRows) => withClue("Rows:")(seenRows should equal(expectedRows))
    }
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

case class ProfilerTestPipe(source: Pipe, name: String, rows: Int, dbAccess: Int)  // MATCH a, ()-[r]->() WHERE id(r) = length(a-->())
                  (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  def planDescription: InternalPlanDescription = source.planDescription.andThen(this.id, name, Set())

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    (0 until dbAccess).foreach(x => state.query.createNode())
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }

  def localEffects: Effects = Effects(WritesAnyNode)

  def symbols: SymbolTable = SymbolTable()

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)
  }
}

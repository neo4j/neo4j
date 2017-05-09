/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.profiler

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.{NestedPipeExpression, ProjectedPath}
import org.neo4j.cypher.internal.compiler.v3_3.pipes._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments.{DbHits, PageCacheHits, PageCacheMisses, Rows}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription._
import org.neo4j.cypher.internal.compiler.v3_3.spi.{EmptyKernelStatisticProvider, KernelStatisticProvider, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.factory.DatabaseInfo

class ProfilerTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("should report simplest case") {
    //GIVEN
    val start = SingleRowPipe()()
    val pipe = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20)
    val queryContext: QueryContext = prepareQueryContext()
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 20)
  }

  test("report page cache statistics for simplest case") {
    //GIVEN
    val start = SingleRowPipe()()
    val statisticProvider = new ConfiguredKernelStatisticProvider()
    val pipe = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20, statisticProvider, hits = 2, misses = 7)
    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 20,
      expectedPageCacheHits = 2, expectedPageCacheMisses = 7)
  }

  private def createPlanDescription(first: (String, Pipe), tail: (String, Pipe)*): InternalPlanDescription = {
    val firstDescr: InternalPlanDescription = PlanDescriptionImpl(first._2.id, first._1, NoChildren, Seq.empty, Set.empty)
    tail.foldLeft(firstDescr) {
      case (descr, (name, pipe)) => PlanDescriptionImpl(pipe.id, name, SingleChild(descr), Seq.empty, Set.empty)
    }
  }

  test("should report multiple pipes case") {
    //GIVEN
    val start = SingleRowPipe()()
    val pipe1 = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25)
    val pipe2 = ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40)
    val pipe3 = ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2)
    val queryContext: QueryContext = prepareQueryContext()
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe1, "bar" -> pipe2, "baz" -> pipe3)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 25)
    assertRecorded(decoratedResult, "bar", expectedRows = 20, expectedDbHits = 40)
    assertRecorded(decoratedResult, "baz", expectedRows = 1, expectedDbHits = 2)
  }

  test("report page cache statistic for multiple pipes case") {
    //GIVEN
    val start = SingleRowPipe()()
    val statisticProvider = new ConfiguredKernelStatisticProvider
    val pipe1 = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25, statisticProvider, 2, 7)
    val pipe2 = ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40, statisticProvider, 12, 35)
    val pipe3 = ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2, statisticProvider, 37, 68)
    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe1, "bar" -> pipe2, "baz" -> pipe3)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 25, expectedPageCacheHits = 2, expectedPageCacheMisses = 7)
    assertRecorded(decoratedResult, "bar", expectedRows = 20, expectedDbHits = 40, expectedPageCacheHits = 10, expectedPageCacheMisses = 28)
    assertRecorded(decoratedResult, "baz", expectedRows = 1, expectedDbHits = 2, expectedPageCacheHits = 25, expectedPageCacheMisses = 33)
  }

  test("should count stuff going through Apply multiple times") {
    val s1 = SingleRowPipe()()
    // GIVEN
    val lhs = ProfilerTestPipe(s1, "lhs", rows = 10, dbAccess = 10)
    val s2 = SingleRowPipe()()
    val rhs = ProfilerTestPipe(s2, "rhs", rows = 20, dbAccess = 30)
    val apply = ApplyPipe(lhs, rhs)()
    val queryContext: QueryContext = prepareQueryContext()
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription(
      "start1" -> s1,
      "start2" -> s2,
      "lhs" -> lhs,
      "rhs" -> rhs,
      "apply" -> apply)

    // WHEN we create the results,
    materialize(apply.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    // THEN
    assertRecorded(decoratedResult, "rhs", expectedRows = 10 * 20, expectedDbHits = 10 * 30)
  }


  test("count dbhits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val start1 = SingleRowPipe()()
    val testPipe = ProfilerTestPipe(start1, "nested pipe", rows = 10, dbAccess = DB_HITS)
    val innerPipe = NestedPipeExpression(testPipe, projectedPath)
    val start2 = SingleRowPipe()()
    val pipeUnderInspection = ProjectionPipe(start2, Map("x" -> innerPipe))()

    val queryContext: QueryContext = prepareQueryContext()
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription(
      "start1" -> start1,
      "start2" -> start2,
      "lhs" -> testPipe,
      "Projection" -> pipeUnderInspection)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS)
  }

  test("count page cache hits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val start1 = SingleRowPipe()()
    val statisticProvider = new ConfiguredKernelStatisticProvider()
    val testPipe = ProfilerTestPipe(start1, "nested pipe", rows = 10, dbAccess = 2, statisticProvider, hits = 3, misses = 4 )
    val innerPipe = NestedPipeExpression(testPipe, projectedPath)
    val start2 = SingleRowPipe()()
    val pipeUnderInspection = ProjectionPipe(start2, Map("x" -> innerPipe))()

    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profiler = new Profiler(DatabaseInfo.HA)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription(
      "start1" -> start1,
      "start2" -> start2,
      "lhs" -> testPipe,
      "Projection" -> pipeUnderInspection)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the page cache hits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = 2, expectedPageCacheHits = 3, expectedPageCacheMisses = 4)
  }

  test("count dbhits for deeply nested NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val start1 = SingleRowPipe()()
    val start2 = SingleRowPipe()()
    val start3 = SingleRowPipe()()
    val profiler1 = ProfilerTestPipe(start1, "nested pipe1", rows = 10, dbAccess = DB_HITS)
    val nestedExpression = NestedPipeExpression(profiler1, projectedPath)
    val innerInnerPipe = ProjectionPipe(start2, Map("y" -> nestedExpression))()
    val profiler2 = ProfilerTestPipe(innerInnerPipe, "nested pipe2", rows = 10, dbAccess = DB_HITS)
    val pipeExpression = NestedPipeExpression(profiler2, projectedPath)
    val pipeUnderInspection = ProjectionPipe(start3, Map("x" -> pipeExpression))()

    val queryContext: QueryContext = prepareQueryContext()
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val description = createPlanDescription(
      "start1" -> start1,
      "start2" -> start2,
      "start3" -> start3,
      "profiler1" -> profiler1,
      "profiler2" -> profiler2,
      "innerInner" -> innerInnerPipe,
      "Projection" -> pipeUnderInspection
    )
    val decoratedResult = profiler.decorate(description, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS * 2)
  }

  test("should not count rows multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler

      val pipe1 = SingleRowPipe()()
      val ctx1: QueryContext = prepareQueryContext()
      val state1 = QueryStateHelper.emptyWith(ctx1, mock[ExternalCSVResource])

      val profiled_1 = profiler.decorate(pipe1, state1)
      val iter1 = Iterator(ExecutionContext.empty, ExecutionContext.empty, ExecutionContext.empty)

      val profiled1 = profiler.decorate(pipe1, iter1)
      profiled1.toList // consume it
      profiled1.asInstanceOf[ProfilingIterator].count should equal(3)

      val pipe2 = SingleRowPipe()()
      val ctx2: QueryContext = prepareQueryContext()
      val state2 = QueryStateHelper.emptyWith(ctx2, mock[ExternalCSVResource])
      val iter2 = Iterator(ExecutionContext.empty, ExecutionContext.empty)

      val profiled_2 = profiler.decorate(pipe2, state2)
      val profiled2 = profiler.decorate(pipe2, iter2)
      profiled2.toList // consume it
      profiled2.asInstanceOf[ProfilingIterator].count should equal(2)
    }

  test("should not count dbhits multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler

      val pipe1 = SingleRowPipe()()
      val ctx1: QueryContext = prepareQueryContext()
      val state1 = QueryStateHelper.emptyWith(ctx1, mock[ExternalCSVResource])

      val profiled1 = profiler.decorate(pipe1, state1)
      profiled1.query.createNode()
      profiled1.query.asInstanceOf[ProfilingPipeQueryContext].count should equal(1)

      val pipe2 = SingleRowPipe()()
      val ctx2: QueryContext = prepareQueryContext()
      val state2 = QueryStateHelper.emptyWith(ctx2, mock[ExternalCSVResource])


      val profiled2 = profiler.decorate(pipe2, state2)
      profiled2.query.createNode()
      profiled2.query.asInstanceOf[ProfilingPipeQueryContext].count should equal(1)
    }

  private def prepareQueryContext(statisticProvider: KernelStatisticProvider = EmptyKernelStatisticProvider) = {
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[QueryTransactionalContext]
    when(queryContext.transactionalContext).thenReturn(transactionalContext)
    when(transactionalContext.kernelStatisticProvider).thenReturn(statisticProvider)
    queryContext
  }

  private def assertRecorded(result: InternalPlanDescription, name: String, expectedRows: Int, expectedDbHits: Int,
                             expectedPageCacheHits: Int = 0, expectedPageCacheMisses: Int = 0) {
    val pipeArgs: Seq[Argument] = result.find(name).flatMap(_.arguments)
    pipeArgs shouldNot be(empty)
    pipeArgs.collect {
      case DbHits(count) => withClue("DbHits:")(count should equal(expectedDbHits))
    }
    pipeArgs.collect {
      case Rows(seenRows) => withClue("Rows:")(seenRows should equal(expectedRows))
    }
    pipeArgs.collect {
      case PageCacheHits(seenHits) => withClue("PageCacheHits:")(seenHits should equal(expectedPageCacheHits))
    }
    pipeArgs.collect {
      case PageCacheMisses(seenMisses) => withClue("PageCacheMisses:")(seenMisses should equal(expectedPageCacheMisses))
    }
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

case class ProfilerTestPipe(source: Pipe, name: String, rows: Int, dbAccess: Int,
                            statisticProvider: ConfiguredKernelStatisticProvider = null, hits: Long = 0, misses: Long
                            = 0)  //
                           // MATCH a, ()-[r]->()
                           // WHERE id(r) = length(a-->())
                  (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  var id = new Id

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    if (statisticProvider != null) {
      statisticProvider.hits = hits
      statisticProvider.misses = misses
    }
    (0 until dbAccess).foreach(x => state.query.createNode())
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }
}

class ConfiguredKernelStatisticProvider extends KernelStatisticProvider {

  var hits:Long = 0
  var misses:Long = 0

  override def getPageCacheHits: Long = hits

  override def getPageCacheMisses: Long = misses
}

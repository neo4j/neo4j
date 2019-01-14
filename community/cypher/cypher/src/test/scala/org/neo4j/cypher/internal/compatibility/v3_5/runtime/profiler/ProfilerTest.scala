/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.planner.v3_5.spi.{EmptyKernelStatisticProvider, KernelStatisticProvider}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.InterpretedCommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{NestedPipeExpression, ProjectedPath}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile}
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.cypher.internal.v3_5.util.attribution.{Id, SequentialIdGen}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ProfilerTest extends CypherFunSuite {

  val idGen = new SequentialIdGen

  test("should report simplest case") {
    //GIVEN
    val start = ArgumentPipe()(idGen.id())
    val pipe = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20)(idGen.id())
    val queryContext: QueryContext = prepareQueryContext()
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe.createResults(queryState))

    //THEN
    assertRecorded(profile, pipe.id, expectedRows = 10, expectedDbHits = 20)
  }

  test("report page cache statistics for simplest case") {
    //GIVEN
    val start = ArgumentPipe()(idGen.id())
    val statisticProvider = new ConfiguredKernelStatisticProvider()
    val pipe = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20, statisticProvider, hits = 2, misses = 7)(idGen.id())
    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe.createResults(queryState))

    //THEN
    assertRecorded(profile, pipe.id, expectedRows = 10, expectedDbHits = 20,
      expectedPageCacheHits = 2, expectedPageCacheMisses = 7)
  }

  test("should report multiple pipes case") {
    //GIVEN
    val start = ArgumentPipe()(idGen.id())
    val pipe1 = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25)(idGen.id())
    val pipe2 = ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40)(idGen.id())
    val pipe3 = ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2)(idGen.id())
    val queryContext: QueryContext = prepareQueryContext()
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.COMMUNITY, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe3.createResults(queryState))

    //THEN
    assertRecorded(profile, pipe1.id, expectedRows = 10, expectedDbHits = 25)
    assertRecorded(profile, pipe2.id, expectedRows = 20, expectedDbHits = 40)
    assertRecorded(profile, pipe3.id, expectedRows = 1, expectedDbHits = 2)
  }

  test("report page cache statistic for multiple pipes case") {
    //GIVEN
    val start = ArgumentPipe()(idGen.id())
    val statisticProvider = new ConfiguredKernelStatisticProvider
    val pipe1 = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25, statisticProvider, 2, 7)(idGen.id())
    val pipe2 = ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40, statisticProvider, 12, 35)(idGen.id())
    val pipe3 = ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2, statisticProvider, 37, 68)(idGen.id())
    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.ENTERPRISE, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe3.createResults(queryState))

    //THEN
    assertRecorded(profile, pipe1.id, expectedRows = 10, expectedDbHits = 25, expectedPageCacheHits = 2, expectedPageCacheMisses = 7)
    assertRecorded(profile, pipe2.id, expectedRows = 20, expectedDbHits = 40, expectedPageCacheHits = 10, expectedPageCacheMisses = 28)
    assertRecorded(profile, pipe3.id, expectedRows = 1, expectedDbHits = 2, expectedPageCacheHits = 25, expectedPageCacheMisses = 33)
  }

  test("should count stuff going through Apply multiple times") {
    val s1 = ArgumentPipe()(idGen.id())
    // GIVEN
    val lhs = ProfilerTestPipe(s1, "lhs", rows = 10, dbAccess = 10)(idGen.id())
    val s2 = ArgumentPipe()(idGen.id())
    val rhs = ProfilerTestPipe(s2, "rhs", rows = 20, dbAccess = 30)(idGen.id())
    val apply = ApplyPipe(lhs, rhs)(idGen.id())
    val queryContext: QueryContext = prepareQueryContext()
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.COMMUNITY, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(apply.createResults(queryState))

    // THEN
    assertRecorded(profile, rhs.id, expectedRows = 10 * 20, expectedDbHits = 10 * 30)
  }

  test("count dbhits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    when(projectedPath.apply(any(), any())).thenReturn(NO_VALUE)
    val DB_HITS = 100
    val start1 = ArgumentPipe()(idGen.id())
    val testPipe = ProfilerTestPipe(start1, "nested pipe", rows = 10, dbAccess = DB_HITS)(idGen.id())
    val innerPipe = NestedPipeExpression(testPipe, projectedPath)
    val start2 = ArgumentPipe()(idGen.id())
    val pipeUnderInspection = ProjectionPipe(start2, InterpretedCommandProjection(Map("x" -> innerPipe)))(idGen.id())

    val queryContext: QueryContext = prepareQueryContext()
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.COMMUNITY, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(profile, pipeUnderInspection.id, expectedRows = 1, expectedDbHits = DB_HITS)
  }

  test("count page cache hits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    when(projectedPath.apply(any(), any())).thenReturn(NO_VALUE)
    val start1 = ArgumentPipe()(idGen.id())
    val statisticProvider = new ConfiguredKernelStatisticProvider()
    val testPipe = ProfilerTestPipe(start1, "nested pipe", rows = 10, dbAccess = 2, statisticProvider, hits = 3, misses = 4 )(idGen.id())
    val innerPipe = NestedPipeExpression(testPipe, projectedPath)
    val start2 = ArgumentPipe()(idGen.id())
    val pipeUnderInspection = ProjectionPipe(start2,InterpretedCommandProjection(Map("x" -> innerPipe)))(idGen.id())

    val queryContext: QueryContext = prepareQueryContext(statisticProvider)
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.HA, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))

    // THEN the ProjectionNewPipe has correctly recorded the page cache hits
    assertRecorded(profile, pipeUnderInspection.id, expectedRows = 1, expectedDbHits = 2, expectedPageCacheHits = 3, expectedPageCacheMisses = 4)
  }

  test("count dbhits for deeply nested NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    when(projectedPath.apply(any(), any())).thenReturn(NO_VALUE)
    val DB_HITS = 100
    val start1 = ArgumentPipe()(idGen.id())
    val start2 = ArgumentPipe()(idGen.id())
    val start3 = ArgumentPipe()(idGen.id())
    val profiler1 = ProfilerTestPipe(start1, "nested pipe1", rows = 10, dbAccess = DB_HITS)(idGen.id())
    val nestedExpression = NestedPipeExpression(profiler1, projectedPath)
    val innerInnerPipe = ProjectionPipe(start2, InterpretedCommandProjection(Map("y" -> nestedExpression)))(idGen.id())
    val profiler2 = ProfilerTestPipe(innerInnerPipe, "nested pipe2", rows = 10, dbAccess = DB_HITS)(idGen.id())
    val pipeExpression = NestedPipeExpression(profiler2, projectedPath)
    val pipeUnderInspection = ProjectionPipe(start3, InterpretedCommandProjection(Map("x" -> pipeExpression)))(idGen.id())

    val queryContext: QueryContext = prepareQueryContext()
    val profile = new InterpretedProfileInformation
    val profiler = new Profiler(DatabaseInfo.COMMUNITY, profile)
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(profile, pipeUnderInspection.id, expectedRows = 1, expectedDbHits = DB_HITS * 2)
  }

  test("should not count rows multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler(DatabaseInfo.COMMUNITY, new InterpretedProfileInformation)

      val pipe1 = ArgumentPipe()(idGen.id())
      val ctx1: QueryContext = prepareQueryContext()
      val state1 = QueryStateHelper.emptyWith(query = ctx1, resources = mock[ExternalCSVResource])

      val profiled_1 = profiler.decorate(pipe1, state1)
      val iter1 = Iterator(ExecutionContext.empty, ExecutionContext.empty, ExecutionContext.empty)

      val profiled1 = profiler.decorate(pipe1, iter1)
      profiled1.toList // consume it
      profiled1.asInstanceOf[ProfilingIterator].count should equal(3)

      val pipe2 = ArgumentPipe()(idGen.id())
      val ctx2: QueryContext = prepareQueryContext()
      val state2 = QueryStateHelper.emptyWith(query = ctx2, resources = mock[ExternalCSVResource])
      val iter2 = Iterator(ExecutionContext.empty, ExecutionContext.empty)

      val profiled_2 = profiler.decorate(pipe2, state2)
      val profiled2 = profiler.decorate(pipe2, iter2)
      profiled2.toList // consume it
      profiled2.asInstanceOf[ProfilingIterator].count should equal(2)
    }

  test("should not count dbhits multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler(DatabaseInfo.COMMUNITY, new InterpretedProfileInformation)

      val pipe1 = ArgumentPipe()(idGen.id())
      val ctx1: QueryContext = prepareQueryContext()
      val state1 = QueryStateHelper.emptyWith(query = ctx1, resources = mock[ExternalCSVResource])

      val profiled1 = profiler.decorate(pipe1, state1)
      profiled1.query.createNode(Array.empty)
      profiled1.query.asInstanceOf[ProfilingPipeQueryContext].count should equal(1)

      val pipe2 = ArgumentPipe()(idGen.id())
      val ctx2: QueryContext = prepareQueryContext()
      val state2 = QueryStateHelper.emptyWith(query = ctx2, resources = mock[ExternalCSVResource])


      val profiled2 = profiler.decorate(pipe2, state2)
      profiled2.query.createNode(Array.empty)
      profiled2.query.asInstanceOf[ProfilingPipeQueryContext].count should equal(1)
    }

  private def prepareQueryContext(statisticProvider: KernelStatisticProvider = EmptyKernelStatisticProvider) = {
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[QueryTransactionalContext]
    when(queryContext.transactionalContext).thenReturn(transactionalContext)
    when(transactionalContext.kernelStatisticProvider).thenReturn(statisticProvider)
    queryContext
  }

  private def assertRecorded(result: QueryProfile,
                             id: Id,
                             expectedRows: Int,
                             expectedDbHits: Int,
                             expectedPageCacheHits: Int = 0,
                             expectedPageCacheMisses: Int = 0) {
    val data: OperatorProfile = result.operatorProfile(id.x)
    withClue("DbHits:")(data.dbHits() should equal(expectedDbHits))
    withClue("Rows:")(data.rows should equal(expectedRows))
    withClue("PageCacheHits:")(data.pageCacheHits() should equal(expectedPageCacheHits))
    withClue("PageCacheMisses:")(data.pageCacheMisses() should equal(expectedPageCacheMisses))
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

case class ProfilerTestPipe(source: Pipe, name: String, rows: Int, dbAccess: Int,
                            statisticProvider: ConfiguredKernelStatisticProvider = null,
                            hits: Long = 0,
                            misses: Long = 0)(val id: Id)
    extends PipeWithSource(source) {

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    if (statisticProvider != null) {
      statisticProvider.hits = hits
      statisticProvider.misses = misses
    }
    (0 until dbAccess).foreach(x => state.query.createNode(Array.empty))
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }
}

class ConfiguredKernelStatisticProvider extends KernelStatisticProvider {

  var hits:Long = 0
  var misses:Long = 0

  override def getPageCacheHits: Long = hits

  override def getPageCacheMisses: Long = misses
}

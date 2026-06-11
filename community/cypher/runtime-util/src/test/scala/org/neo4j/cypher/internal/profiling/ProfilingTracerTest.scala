/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.profiling

import org.mockito.Answers
import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapper
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.impl.query.statistic.StatisticProvider

class ProfilingTracerTest extends RuntimeUtilTestSuite {

  class Clock extends ProfilingTracer.Clock {
    var nanoTime: Long = 0L

    def progress(nanos: Long): Unit = {
      assert(nanos > 0, "time must move forwards")
      nanoTime += nanos
    }
  }

  private val id = Id.INVALID_ID
  private val swapper: PageSwapper = mock[PageSwapper](Answers.RETURNS_MOCKS)

  test("shouldReportExecutionTimeOfQueryExecution") {
    // given
    val clock = new Clock
    val operatorId = id
    val tracer = new ProfilingTracer(clock, NoKernelStatisticProvider)
    val event = tracer.executeOperator(operatorId)

    // when
    clock.progress(516)
    event.close()

    // then
    tracer.timeOf(operatorId) should equal(516)
  }

  test("multiple uses of the same Id should aggregate spent time") {
    // given
    val clock = new Clock
    val operatorId = id
    val tracer = new ProfilingTracer(clock, NoKernelStatisticProvider)

    // when
    val event1 = tracer.executeOperator(operatorId)
    clock.progress(12)
    event1.close()

    val event2 = tracer.executeOperator(operatorId)
    clock.progress(45)
    event2.close()

    // then
    tracer.timeOf(operatorId) should equal(12 + 45)
  }

  test("multiple uses of the same Id with NO_DATA should produce NO_DATA") {
    // given
    val clock = new Clock
    val operatorId = id
    val tracer = new ProfilingTracer(clock, NoKernelStatisticProvider)

    // when
    val event1 = tracer.executeOperator(operatorId, false)
    clock.progress(12)
    event1.dbHits(OperatorProfile.NO_DATA.toInt)
    event1.rows(OperatorProfile.NO_DATA.toInt)
    event1.close()

    val event2 = tracer.executeOperator(operatorId, false)
    clock.progress(45)
    event2.dbHits(OperatorProfile.NO_DATA.toInt)
    event2.rows(OperatorProfile.NO_DATA.toInt)
    event2.close()

    // then
    val profile = tracer.operatorProfile(operatorId.x)
    profile.time() should equal(OperatorProfile.NO_DATA)
    profile.dbHits() should equal(OperatorProfile.NO_DATA)
    profile.rows() should equal(OperatorProfile.NO_DATA)
    profile.pageCacheHits() should equal(OperatorProfile.NO_DATA)
    profile.pageCacheMisses() should equal(OperatorProfile.NO_DATA)
  }

  test("shouldReportDbHitsOfQueryExecution") {
    // given
    val operatorId = id
    val tracer = new ProfilingTracer(NoKernelStatisticProvider)
    val event = tracer.executeOperator(operatorId)

    // when
    (0 until 516).foreach { _ =>
      event.dbHit()
    }

    event.close()

    // then
    tracer.dbHitsOf(operatorId) should equal(516)
  }

  test("shouldReportRowsOfQueryExecution") {
    // given
    val operatorId = id
    val tracer = new ProfilingTracer(NoKernelStatisticProvider)
    val event = tracer.executeOperator(operatorId)

    // when
    (0 until 516).foreach { _ =>
      event.row()
    }

    event.close()

    // then
    tracer.rowsOf(operatorId) should equal(516)

  }

  test("report page cache hits as part of profiling statistics") {
    val operatorId = id
    val cursorTracer = new DefaultPageCursorTracer(new DefaultPageCacheTracer(), "test")
    val tracer = new ProfilingTracer(new DelegatingKernelStatisticProvider(cursorTracer))
    val event = tracer.executeOperator(operatorId)

    1 to 100 foreach { _ =>
      {
        val pin = cursorTracer.beginPin(false, 1, swapper)
        pin.hit()
        pin.close()
        cursorTracer.unpin(1, swapper)
      }
    }

    event.close()

    val information = tracer.operatorProfile(operatorId.x)
    information.pageCacheHits() should equal(100)
  }

  test("report page cache misses as part of profiling statistics") {
    val operatorId = id
    val cursorTracer = new DefaultPageCursorTracer(new DefaultPageCacheTracer(), "test")
    val tracer = new ProfilingTracer(new DelegatingKernelStatisticProvider(cursorTracer))
    val event = tracer.executeOperator(operatorId)

    1 to 17 foreach { _ =>
      {
        val pin = cursorTracer.beginPin(false, 1, swapper)
        val pageFault = pin.beginPageFault(1, swapper)
        pageFault.close()
        pin.close()
        cursorTracer.unpin(1, swapper)
      }
    }

    event.close()

    val information = tracer.operatorProfile(operatorId.x)
    information.pageCacheMisses() should equal(17)
  }

  test("report index usage as part of profiling statistics") {
    val operatorId = id
    val tracer = new ProfilingTracer(NoKernelStatisticProvider)
    val event = tracer.executeOperator(operatorId)
    val index1 = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1)).withName("index1").materialise(1)
    val index2 = IndexPrototype.forSchema(SchemaDescriptors.forLabel(2, 2)).withName("index2").materialise(2)

    event.indexHit(index1)
    event.indexHit(index1)
    event.indexHit(index2)
    event.close()

    val information = tracer.operatorProfile(operatorId.x)
    information.indexesUsed() should equal(Array(index1, index2))
    information.indexUseCount() should equal(Array(2, 1))
  }

  test("report zero index usage as part of profiling statistics") {
    val operatorId = id
    val tracer = new ProfilingTracer(NoKernelStatisticProvider)
    tracer.executeOperator(operatorId).close() // no profiling sent

    val information = tracer.operatorProfile(operatorId.x)
    information.indexesUsed() shouldBe empty
    information.indexUseCount() shouldBe empty
  }

  class DelegatingKernelStatisticProvider(tracer: DefaultPageCursorTracer) extends StatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }

  object NoKernelStatisticProvider extends StatisticProvider {
    override def getPageCacheHits: Long = OperatorProfile.NO_DATA

    override def getPageCacheMisses: Long = OperatorProfile.NO_DATA
  }
}

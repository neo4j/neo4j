/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.v3_4.codegen.profiling

import org.neo4j.cypher.internal.planner.v3_4.spi.{EmptyKernelStatisticProvider, KernelStatisticProvider}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer

class ProfilingTracerTest extends CypherFunSuite {

  class Clock extends ProfilingTracer.Clock {
    var nanoTime: Long = 0L

    def progress(nanos: Long) {
      assert(nanos > 0, "time must move forwards")
      nanoTime += nanos
    }
  }

  val id = Id.INVALID_ID

  test("shouldReportExecutionTimeOfQueryExecution") {
    // given
    val clock = new Clock
    val operatorId = id
    val tracer = new ProfilingTracer(clock, EmptyKernelStatisticProvider)
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
    val tracer = new ProfilingTracer(clock, EmptyKernelStatisticProvider)

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

  test("shouldReportDbHitsOfQueryExecution") {
    // given
    val operatorId = id
    val tracer = new ProfilingTracer(EmptyKernelStatisticProvider)
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
    val tracer = new ProfilingTracer(EmptyKernelStatisticProvider)
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
    val cursorTracer = new DefaultPageCursorTracer
    val tracer = new ProfilingTracer(new DelegatingKernelStatisticProvider(cursorTracer))
    val event = tracer.executeOperator(operatorId)

    1 to 100 foreach { _ => {
        val pin = cursorTracer.beginPin(false, 1, null)
        pin.hit()
        pin.done()
      }
    }

    event.close()

    val information = tracer.get(operatorId)
    information.pageCacheHits() should equal(100)
  }

  test("report page cache misses as part of profiling statistics") {
    val operatorId = id
    val cursorTracer = new DefaultPageCursorTracer
    val tracer = new ProfilingTracer(new DelegatingKernelStatisticProvider(cursorTracer))
    val event = tracer.executeOperator(operatorId)

    1 to 17 foreach { _ => {
      val pin = cursorTracer.beginPin(false, 1, null)
      val pageFault = pin.beginPageFault()
      pageFault.done()
      pin.done()
    }
    }

    event.close()

    val information = tracer.get(operatorId)
    information.pageCacheMisses() should equal(17)
  }

  class DelegatingKernelStatisticProvider(tracer: DefaultPageCursorTracer) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }
}

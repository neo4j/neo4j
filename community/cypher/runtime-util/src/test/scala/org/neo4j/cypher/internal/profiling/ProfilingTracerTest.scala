/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.profiling

import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer

class ProfilingTracerTest extends CypherFunSuite {

  class Clock extends ProfilingTracer.Clock {
    var nanoTime: Long = 0L

    def progress(nanos: Long) {
      assert(nanos > 0, "time must move forwards")
      nanoTime += nanos
    }
  }

  private val id = Id.INVALID_ID

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
    profile.pageCacheHitRatio() should equal(OperatorProfile.NO_DATA)
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

    val information = tracer.operatorProfile(operatorId.x)
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

    val information = tracer.operatorProfile(operatorId.x)
    information.pageCacheMisses() should equal(17)
  }

  class DelegatingKernelStatisticProvider(tracer: DefaultPageCursorTracer) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }
}

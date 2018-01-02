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
package org.neo4j.cypher.internal.tracing

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import org.mockito.Mockito.verify
import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase.{LOGICAL_PLANNING, PARSING}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{closing, using}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.tracing.CompilationTracer.NO_COMPILATION_TRACING
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer.QueryEvent

class TimingCompilationTracerTest extends CypherFunSuite {

  test("no-op tracing") {
    // when
    compile(new FakeClock, NO_COMPILATION_TRACING, "MATCH (n) RETURN n")
    // then - no exceptions should be thrown
  }

  test("measure time") {
    // given
    val clock = new FakeClock
    val listener = mock[TimingCompilationTracer.EventListener]

    // when
    compile(clock, new TimingCompilationTracer(clock, listener), "MATCH (n) RETURN n")

    // then
    val argumentCaptor = argCaptor[QueryEvent]
    verify(listener).queryCompiled(argumentCaptor.capture())
    val event = argumentCaptor.getValue
    event.nanoTime() should equal(227 * 1000 * 1000)
    event.query() should equal("MATCH (n) RETURN n")
    val phases = event.phases()
    phases.size() should equal(2)
    phases.get(0).phase() should equal(PARSING)
    phases.get(0).nanoTime() should equal(11 * 1000 * 1000)
    phases.get(1).phase() should equal(LOGICAL_PLANNING)
    phases.get(1).nanoTime() should equal(216 * 1000 * 1000)
  }

  def compile(clock: FakeClock, tracer: CompilationTracer, query: String): Unit = {
    using(tracer.compileQuery(query)) { event =>
      closing(event.beginPhase(PARSING)) {
        clock.progress(11, MILLISECONDS)
      }
      closing(event.beginPhase(LOGICAL_PLANNING)) {
        clock.progress(216, MILLISECONDS)
      }
    }
  }
}

class FakeClock extends TimingCompilationTracer.Clock {
  private var time: Long = 0

  def nanoTime: Long = time

  def progress(time: Long, unit: TimeUnit) = {
    this.time += unit.toNanos(time)
  }
}
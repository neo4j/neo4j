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
package org.neo4j.cypher.internal.procs

import org.mockito.Mockito.when
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.kernel.api.query.RuntimeName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters.SetHasAsJava

class ChainedExecutionPlanTest extends CommunityCypherTestSuite {

  private val ctxMock: SystemUpdateCountingQueryContext = mock[SystemUpdateCountingQueryContext]
  when(ctxMock.getStatistics).thenReturn(QueryStatistics())

  private val originalCtx: QueryContext = mock[QueryContext]
  private val subscriber: QuerySubscriber = new QuerySubscriberAdapter {}
  private val params: MapValue = VirtualValues.EMPTY_MAP
  private val input: InputDataStream = null
  private val mode: ExecutionMode = NormalMode

  /**
   * A minimal ChainedExecutionPlan whose runSpecific returns a fixed result and (optionally)
   * records the ctx it received.
   */
  private class StubPlan(
    src: Option[ExecutionPlan],
    contextForCreate: SystemUpdateCountingQueryContext,
    result: => RuntimeResult,
    received: Option[AtomicReference[SystemUpdateCountingQueryContext]] = None
  ) extends ChainedExecutionPlan[SystemUpdateCountingQueryContext](src) {

    override def runSpecific(
      ctx: SystemUpdateCountingQueryContext,
      executionMode: ExecutionMode,
      params: MapValue,
      prePopulateResults: Boolean,
      subscriber: QuerySubscriber,
      previousNotifications: Set[InternalNotification]
    ): RuntimeResult = {
      received.foreach(_.set(ctx))
      result
    }

    override def createContext(orig: QueryContext): SystemUpdateCountingQueryContext = contextForCreate
    override def querySubscriber(c: SystemUpdateCountingQueryContext, s: QuerySubscriber): QuerySubscriber = s
    override def runtimeName: RuntimeName = RuntimeName.SYSTEM
  }

  /**
   * Build a chain of `depth` StubPlans, all returning `result` from runSpecific.
   * Innermost plan has no source.
   */
  private def buildChain(depth: Int, result: RuntimeResult): ChainedExecutionPlan[SystemUpdateCountingQueryContext] = {
    val innermost: ChainedExecutionPlan[SystemUpdateCountingQueryContext] = new StubPlan(None, ctxMock, result)
    (1 until depth).foldLeft(innermost) { (src, _) => new StubPlan(Some(src), ctxMock, result) }
  }

  test("deep chain does not stack overflow on regular RuntimeResult") {
    val depth = 10000
    val expected = new TestRuntimeResult
    val chain = buildChain(depth, expected)

    val result = chain.run(originalCtx, mode, params, prePopulateResults = false, input, subscriber)

    // Every layer's runSpecific returns `expected`, so the final unwound result is `expected`.
    result should be(expected)
  }

  test("deep chain propagates IgnoredRuntimeResult through onSkip at every layer") {
    val depth = 10000
    val notification = mock[InternalNotification]
    val innermost = new StubPlan(None, ctxMock, IgnoredRuntimeResult(Set(notification)))
    val chain = (1 until depth).foldLeft[ChainedExecutionPlan[SystemUpdateCountingQueryContext]](innermost) {
      // Outer layers return a regular result — but the original recursive code's IgnoredRuntimeResult
      // short-circuit means runSpecific must not be called on outer layers. We verify this by making
      // outer runSpecific throw if invoked.
      (src, _) =>
        new StubPlan(
          Some(src),
          ctxMock,
          throw new AssertionError("runSpecific should not be called when source returned IgnoredRuntimeResult")
        )
    }

    val result = chain.run(originalCtx, mode, params, prePopulateResults = false, input, subscriber)

    result shouldBe an[IgnoredRuntimeResult]
    result.asInstanceOf[IgnoredRuntimeResult].runtimeNotifications should contain(notification)
  }

  test("UpdatingSystemCommandRuntimeResult.ctx replaces the local ctx at the next layer") {
    val specialCtx: SystemUpdateCountingQueryContext = mock[SystemUpdateCountingQueryContext]
    when(specialCtx.getStatistics).thenReturn(QueryStatistics())

    val received = new AtomicReference[SystemUpdateCountingQueryContext]()
    val innerResult = UpdatingSystemCommandRuntimeResult(specialCtx, None, Set.empty)
    val finalResult = new TestRuntimeResult

    val innermost = new StubPlan(None, ctxMock, innerResult)
    val outer = new StubPlan(Some(innermost), ctxMock, finalResult, received = Some(received))

    val result = outer.run(originalCtx, mode, params, prePopulateResults = false, input, subscriber)

    // The outer layer's runSpecific must have received the inner result's ctx, not its own createContext output.
    received.get() should be theSameInstanceAs specialCtx
    result should be(finalResult)
  }

  test("single-element chain with no source calls runSpecific once with empty previous notifications") {
    val expected = new TestRuntimeResult
    val plan = new StubPlan(None, ctxMock, expected)

    val result = plan.run(originalCtx, mode, params, prePopulateResults = false, input, subscriber)

    result should be(expected)
  }

  /**
   * A bare-minimum RuntimeResult used as a marker in the tests above. Matches no special case
   * (not IgnoredRuntimeResult, not UpdatingSystemCommandRuntimeResult) so it exercises the
   * generic "Some(other RuntimeResult)" branch.
   */
  private class TestRuntimeResult extends RuntimeResult {
    override def fieldNames(): Array[String] = Array.empty
    override def hasServedRows: Boolean = false
    override def queryStatistics(): QueryStatistics = QueryStatistics()
    override def consumptionState: ConsumptionState = ConsumptionState.EXHAUSTED
    override def close(): Unit = {}
    override def queryProfile(): QueryProfile = QueryProfile.NONE
    override def request(numberOfRecords: Long): Unit = {}
    override def cancel(): Unit = {}
    override def await(): Boolean = false
    override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED
    override def notifications(): util.Set[InternalNotification] = Set.empty[InternalNotification].asJava
    override def getErrorOrNull: Throwable = null
  }
}

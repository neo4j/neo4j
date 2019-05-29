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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.{CypherException, CypherExecutionException}
import org.neo4j.graphdb.Notification
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySubscriberAdapter}

class ClosingExecutionResultTest extends CypherFunSuite {

  private val query: ExecutingQuery = mock[ExecutingQuery]

  // HAPPY CLOSING

  test("should report end-of-query on pre-closed inner") {
    // given
    val inner = new NiceInner(Array.empty)
    val monitor = AssertableMonitor()
    inner.close()

    // when
    ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor, DO_NOTHING_SUBSCRIBER)

    // then
    monitor.assertSuccess(query)
  }

  // EXPLOSIVE CLOSING

  test("should close on exploding initiate") {
    // given
    val inner = new ExplodingInner(alreadyInInitiate = true)
    val monitor = AssertableMonitor()

    // when
    intercept[TestOuterException] {
      ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor, DO_NOTHING_SUBSCRIBER)
    }

    // then
    inner.isClosed should equal(true)
    monitor.assertError(query, TestOuterException("initiate"))
  }

  test("should close on exploding fieldNames") {
    assertCloseOnExplodingMethod(_.fieldNames(), "fieldNames")
  }

  test("should close on exploding executionMode") {
    assertCloseOnExplodingMethod(_.executionMode, "executionMode")
  }

  test("should close on exploding executionPlanDescription") {
    assertCloseOnExplodingMethod(_.executionPlanDescription(), "executionPlanDescription")
  }

  test("should close on exploding queryType") {
    assertCloseOnExplodingMethod(_.queryType, "queryType")
  }

  test("should close on exploding notifications") {
    assertCloseOnExplodingMethod(_.notifications, "notifications")
  }

  private def assertCloseOnExplodingMethod(f: ClosingExecutionResult => Unit,
                                           errorMsg: String,
                                           iteratorMode: IteratorMode = DIRECT_EXPLODE): Unit = {
    // given
    val inner = new ExplodingInner(iteratorMode = iteratorMode)
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor, throwingSubscriber)

    // when
    intercept[TestOuterException] { f(x) }

    // then
    val expectedException = TestOuterException(errorMsg)
    inner.closeReason should equal(Error(expectedException))
    monitor.assertError(query, expectedException)
  }

  // EXPLOSIONS DURING CLOSE

  test("should report explosion on close") {
    // given
    val inner = new ExplodingInner(alsoExplodeOnClose = true)
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor, DO_NOTHING_SUBSCRIBER)

    // when
    intercept[TestOuterException] { x.close() }

    // then
    monitor.assertError(query, TestOuterException("close"))
  }

  // HELPERS
  private def throwingSubscriber = new QuerySubscriberAdapter {
    override def onError(throwable: Throwable): Unit = throw throwable
  }

  abstract class ClosingInner extends InternalExecutionResult {

    var closeReason: CloseReason = _
    override def isClosed: Boolean = closeReason != null

    override def close(reason: CloseReason): Unit =
      closeReason = reason
  }

  class NiceInner(values: Array[Int]) extends ClosingInner {

    self =>

    override def initiate(): Unit = {}

    override def executionMode: ExecutionMode = null

    override def executionPlanDescription(): InternalPlanDescription = null

    override def queryType: InternalQueryType = null

    override def notifications: Iterable[Notification] = null

    override def fieldNames(): Array[String] = Array("x", "y")

    override def request(numberOfRecords: Long): Unit = {
    }

    override def cancel(): Unit = {}

    override def await(): Boolean = true
  }

  class ExplodingInner(alreadyInInitiate: Boolean = false,
                       iteratorMode: IteratorMode = DIRECT_EXPLODE,
                       alsoExplodeOnClose: Boolean = false) extends ClosingInner {

    self =>

    override def initiate(): Unit = if (alreadyInInitiate) throw TestInnerException("initiate")

    override def executionMode: ExecutionMode = throw TestInnerException("executionMode")

    override def executionPlanDescription(): InternalPlanDescription = throw TestInnerException("executionPlanDescription")

    override def queryType: InternalQueryType = throw TestInnerException("queryType")

    override def notifications: Iterable[Notification] = throw TestInnerException("notifications")

    override def close(reason: CloseReason): Unit = {
      super.close(reason)
      if (alsoExplodeOnClose) throw TestInnerException("close")
    }

    override def fieldNames(): Array[String] = throw TestInnerException("fieldNames")

    override def request(numberOfRows: Long): Unit = throw TestInnerException("request")

    override def cancel(): Unit = throw TestInnerException("cancel")

    override def await(): Boolean = throw TestInnerException("await")
  }

  sealed trait IteratorMode
  case object DIRECT_EXPLODE extends IteratorMode
  case object HAS_NEXT_EXPLODE extends IteratorMode
  case object NEXT_EXPLODE extends IteratorMode

  private val testRunSafely = new RunSafely {
    override def apply[T](body: => T)(implicit f: Throwable => T): T = {
      try {
        body
      } catch {
        case t: TestInnerException =>
          f(TestOuterException(t.msg))

        case t: Throwable =>
          f(new CypherExecutionException(t.getMessage, t))
      }
    }
  }

  case class AssertableMonitor() extends QueryExecutionMonitor {

    private var nCalls = 0
    private var query: ExecutingQuery = _
    private var reason: CloseReason = _

    override def endFailure(query: ExecutingQuery, failure: Throwable): Unit = {
      this.query = query
      this.reason = if (failure == null) Failure else Error(failure)
      nCalls += 1
    }

    override def endSuccess(query: ExecutingQuery): Unit = {
      this.query = query
      this.reason = Success
      nCalls += 1
    }

    def assertSuccess(query: ExecutingQuery): Unit = {
      this.reason should equal(Success)
      this.query should equal(query)
      this.nCalls should equal(1)
    }

    def assertFailure(query: ExecutingQuery): Unit = {
      this.reason should equal(Failure)
      this.query should equal(query)
      this.nCalls should equal(1)
    }

    def assertError(query: ExecutingQuery, throwable: Throwable): Unit = {
      this.reason should equal(Error(throwable))
      this.query should equal(query)
      this.nCalls should equal(1)
    }
  }
}

abstract class TestException(msg: String) extends CypherException(msg, null) {
  override def status: Status = Status.General.UnknownError
  override def toString: String = s"${getClass.getSimpleName}($msg)"
}

case class TestInnerException(msg: String) extends TestException(msg)
case class TestOuterException(msg: String) extends TestException(msg)

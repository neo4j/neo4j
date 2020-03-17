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
package org.neo4j.cypher.internal.compatibility

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryResult
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, ResourceIterator, Result}
import org.neo4j.helpers.collection.Iterators
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

class ClosingExecutionResultTest extends CypherFunSuite {

  private val query: ExecutingQuery = mock[ExecutingQuery]

  // HAPPY CLOSING

  test("should report end-of-query on pre-closed inner") {
    // given
    val inner = new NiceInner(Nil)
    val monitor = AssertableMonitor()
    inner.close()

    // when
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)

    // then
    monitor.assertSuccess(query)
  }

  test("should close after accept I") {
    assertClosedAfterConsumption(_.accept(mock[ResultVisitor[Exception]]))
  }

  test("should close after accept II") {
    assertClosedAfterConsumption(_.accept(mock[QueryResult.QueryResultVisitor[Exception]]))
  }

  test("should close after javaColumnAs") {
    assertClosedAfterConsumption(result => Iterators.count(result.javaColumnAs[Int]("x")))
  }

  test("should close after javaIterator") {
    assertClosedAfterConsumption(result => Iterators.count(result.javaIterator))
  }

  test("should close after javaColumnAs.close") {
    assertClosedAfterConsumption(_.javaColumnAs[Int]("x").close())
  }

  test("should close after javaIterator.close") {
    assertClosedAfterConsumption(_.javaIterator.close())
  }

  test("should close after dumpToString I") {
    assertClosedAfterConsumption(_.dumpToString())
  }

  test("should close after dumpToString II") {
    assertClosedAfterConsumption(_.dumpToString(mock[PrintWriter]))
  }

  private def assertClosedAfterConsumption(f: ClosingExecutionResult => Unit): Unit = {
    // given
    val inner = new NiceInner(List(1, 2))
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)

    // when
    f(x)

    // then
    inner.closeReason should equal(Success)
    monitor.assertSuccess(query)
  }

  // EXPLOSIVE CLOSING

  test("should close on exploding initiate") {
    // given
    val inner = new ExplodingInner(alreadyInInitiate = true)
    val monitor = AssertableMonitor()

    // when
    intercept[TestOuterException] {
      ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)
    }

    // then
    inner.isClosed should equal(true)
    monitor.assertError(query, TestInnerException("initiate"))
  }

  test("should close on exploding fieldNames") {
    assertCloseOnExplodingMethod(_.fieldNames(), "fieldNames")
  }

  test("should close on exploding javaColumns") { // delegates to fieldNames
    assertCloseOnExplodingMethod(_.javaColumns, "fieldNames")
  }

  test("should close on exploding queryStatistics") {
    assertCloseOnExplodingMethod(_.queryStatistics(), "queryStatistics")
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

  test("should close on exploding accept I") {
    assertCloseOnExplodingMethod(_.accept(mock[ResultVisitor[Exception]]), "accept")
  }

  test("should close on exploding accept II") {
    assertCloseOnExplodingMethod(_.accept(mock[QueryResult.QueryResultVisitor[Exception]]), "accept")
  }

  test("should close on exploding executionType") { // delegates to queryType
    assertCloseOnExplodingMethod(_.executionType, "queryType")
  }

  test("should close on exploding dumpToString I") {
    assertCloseOnExplodingMethod(_.dumpToString(), "dumpToString")
  }

  test("should close on exploding dumpToString II") {
    assertCloseOnExplodingMethod(_.dumpToString(mock[PrintWriter]), "dumpToString")
  }

  test("should close on exploding at create time javaColumnAs") {
    assertCloseOnExplodingMethod(_.javaColumnAs[Int]("x"), "DIRECT_EXPLODE")
  }

  test("should close on exploding at create time javaIterator") {
    assertCloseOnExplodingMethod(_.javaIterator, "DIRECT_EXPLODE")
  }

  test("should close on exploding at hasNext time javaColumnAs") {
    assertCloseOnExplodingMethod(_.javaColumnAs[Int]("x").next(), "HAS_NEXT_EXPLODE", HAS_NEXT_EXPLODE)
  }

  test("should close on exploding at hasNext time javaIterator") {
    assertCloseOnExplodingMethod(_.javaIterator.next(), "HAS_NEXT_EXPLODE", HAS_NEXT_EXPLODE)
  }

  test("should close on exploding at next time javaColumnAs") {
    assertCloseOnExplodingMethod(_.javaColumnAs[Int]("x").next(), "NEXT_EXPLODE", NEXT_EXPLODE)
  }

  test("should close on exploding at next time javaIterator") {
    assertCloseOnExplodingMethod(_.javaIterator.next(), "NEXT_EXPLODE", NEXT_EXPLODE)
  }

  private def assertCloseOnExplodingMethod(f: ClosingExecutionResult => Unit,
                                           errorMsg: String,
                                           iteratorMode: IteratorMode = DIRECT_EXPLODE): Unit = {
    // given
    val inner = new ExplodingInner(iteratorMode = iteratorMode)
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)

    // when
    intercept[TestOuterException] { f(x) }

    // then
    val expectedException = TestInnerException(errorMsg)
    inner.closeReason should equal(Error(expectedException))
    monitor.assertError(query, expectedException)
  }

  // EXPLOSIONS DURING CLOSE

  test("should report explosion on close") {
    // given
    val inner = new ExplodingInner(alsoExplodeOnClose = true)
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)

    // when
    intercept[TestOuterException] { x.close() }

    // then
    monitor.assertError(query, TestInnerException("close"))
  }

  test("should suppress explosion on close if already exploded") {
    // given
    val inner = new ExplodingInner(alsoExplodeOnClose = true)
    val monitor = AssertableMonitor()
    val x = ClosingExecutionResult.wrapAndInitiate(query, inner, testRunSafely, monitor)

    // when
    intercept[TestOuterException] { x.accept(mock[ResultVisitor[Exception]]) }

    // then
    val initialException = TestInnerException("accept")
    monitor.assertError(query, initialException)
    inner.closeReason match {
      case Error(t) =>
        t should equal(initialException)
        t.getSuppressed should contain(TestInnerException("close"))

      case wrongReason =>
        wrongReason should equal(initialException)
    }
  }

  // HELPERS

  abstract class ClosingInner extends InternalExecutionResult {

    var closeReason: CloseReason = _

    override def isClosed: Boolean = closeReason != null

    override def close(reason: CloseReason): Unit =
      closeReason = reason
  }

  class NiceInner(values: Seq[Int]) extends ClosingInner {

    self =>

    override def initiate(): Unit = {}

    override def javaColumnAs[T](column: String): ResourceIterator[T] = StubResourceInterator[T]()

    override def javaIterator: ResourceIterator[util.Map[String, AnyRef]] = StubResourceInterator()

    override def dumpToString(writer: PrintWriter): Unit = writer.print(dumpToString())

    override def dumpToString(): String =
      fieldNames().mkString("", ",", "\n") +
      values.map(x => s"$x,$x").mkString("\n")

    override def queryStatistics(): QueryStatistics = null

    override def executionMode: ExecutionMode = null

    override def executionPlanDescription(): InternalPlanDescription = null

    override def queryType: InternalQueryType = null

    override def notifications: Iterable[Notification] = null

    override def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = {}

    override def fieldNames(): Array[String] = Array("x", "y")

    override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit = {}

    case class StubResourceInterator[T]() extends ResourceIterator[T] {

      private var i = 0

      override def close(): Unit = self.close(Success)

      override def hasNext: Boolean = i < values.size

      override def next(): T = {
        i += 1
        null.asInstanceOf[T]
      }
    }
  }

  class ExplodingInner(alreadyInInitiate: Boolean = false,
                       iteratorMode: IteratorMode = DIRECT_EXPLODE,
                       alsoExplodeOnClose: Boolean = false) extends ClosingInner {

    self =>

    override def initiate(): Unit = if (alreadyInInitiate) throw TestInnerException("initiate")

    override def javaColumnAs[T](column: String): ResourceIterator[T] = StubResourceIterator[T]()

    override def javaIterator: ResourceIterator[util.Map[String, AnyRef]] = StubResourceIterator()

    override def dumpToString(writer: PrintWriter): Unit = throw TestInnerException("dumpToString")

    override def dumpToString(): String = throw TestInnerException("dumpToString")

    override def queryStatistics(): QueryStatistics = throw TestInnerException("queryStatistics")

    override def executionMode: ExecutionMode = throw TestInnerException("executionMode")

    override def executionPlanDescription(): InternalPlanDescription = throw TestInnerException("executionPlanDescription")

    override def queryType: InternalQueryType = throw TestInnerException("queryType")

    override def notifications: Iterable[Notification] = throw TestInnerException("notifications")

    override def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = throw TestInnerException("accept")

    override def close(reason: CloseReason): Unit = {
      super.close(reason)
      if (alsoExplodeOnClose) throw TestInnerException("close")
    }

    override def fieldNames(): Array[String] = throw TestInnerException("fieldNames")

    override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit = throw TestInnerException("accept")

    case class StubResourceIterator[T]() extends ResourceIterator[T] {

      if (iteratorMode == DIRECT_EXPLODE) throw TestInnerException(iteratorMode.toString)

      private var i = 0

      override def close(): Unit = self.close(Success)

      override def hasNext: Boolean =
        if (iteratorMode == HAS_NEXT_EXPLODE)
          throw TestInnerException(iteratorMode.toString)
        else true

      override def next(): T = // guaranteed NEXT_EXPLODE
        throw TestInnerException(iteratorMode.toString)
    }
  }

  sealed trait IteratorMode
  case object DIRECT_EXPLODE extends IteratorMode
  case object HAS_NEXT_EXPLODE extends IteratorMode
  case object NEXT_EXPLODE extends IteratorMode

  private val testRunSafely = new RunSafely {
    override def apply[T](body: => T)(implicit f: ExceptionHandler): T = {
      try {
        body
      } catch {
        case t: TestInnerException =>
          f(t)
          throw TestOuterException(t.msg)

        case t: Throwable =>
          f(t)
          throw t
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

    override def endFailure(query: ExecutingQuery, reason: String): Unit = {
      this.query = query
      this.reason = Failure
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

trait TestException extends Throwable {
  def msg:String
  override def toString: String = s"${getClass.getSimpleName}($msg)"
}
case class TestInnerException(msg: String) extends TestException
case class TestOuterException(msg: String) extends TestException

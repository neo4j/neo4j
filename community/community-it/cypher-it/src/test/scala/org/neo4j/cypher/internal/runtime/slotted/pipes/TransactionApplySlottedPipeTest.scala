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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.Mockito.when
import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.slotted.SlottedCypherRowFactory
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.scalatestplus.mockito.MockitoSugar.mock

class TransactionApplySlottedPipeTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("should close on failure with ON ERROR FAIL") {
    val slots = SlotConfiguration.empty
      .newReference("status", nullable = true, CTMap)

    val lhs = FakeSlottedPipe(slots, Seq(Map(), Map(), Map()))
    val rhs = FakeSlottedPipe(slots, Seq(Map(), Map()), new FailingNextIterable(Map(), Map()), Seq(Map()))

    val pipe = TransactionApplySlottedPipe(lhs, rhs, literal(1), OnErrorFail, Set.empty, slots.get("status"))()

    withQueryState(IMPLICIT) { state =>
      state.setExecutionContextFactory(SlottedCypherRowFactory(slots, slots.size()))
      pipe.rowFactory = SlottedCypherRowFactory(slots, slots.size())

      val result = pipe.createResults(state)

      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      val exception = intercept[Exception](result.next())
      exception.getMessage shouldBe "Hello, I fail for you"

      rhs.allWasClosed shouldBe true

      result.close()
      lhs.allWasClosed shouldBe true
    }
  }

  test("should close on failure with ON ERROR BREAK") {
    val slots = SlotConfiguration.empty
      .newReference("status", nullable = true, CTMap)

    val lhs = FakeSlottedPipe(slots, Seq(Map(), Map(), Map()))
    val rhs = FakeSlottedPipe(slots, Seq(Map(), Map()), new FailingNextIterable(Map(), Map()), Seq(Map(), Map()))

    val pipe = TransactionApplySlottedPipe(lhs, rhs, literal(1), OnErrorBreak, Set.empty, slots.get("status"))()

    withQueryState(IMPLICIT) { state =>
      state.setExecutionContextFactory(SlottedCypherRowFactory(slots, slots.size()))
      pipe.rowFactory = SlottedCypherRowFactory(slots, slots.size())

      val result = pipe.createResults(state)

      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertFailedBatch(result.next())
      result.hasNext shouldBe true
      assertSkippedBatch(result.next())

      result.hasNext shouldBe false

      rhs.allWasClosed shouldBe true
      lhs.allWasClosed shouldBe true
    }
  }

  test("should close on failure with ON ERROR CONTINUE") {
    val slots = SlotConfiguration.empty
      .newReference("status", nullable = true, CTMap)

    val lhs = FakeSlottedPipe(slots, Seq(Map(), Map(), Map(), Map()))
    val rhs = FakeSlottedPipe(
      slots,
      Seq(Map(), Map()),
      new FailingNextIterable(Map(), Map()),
      Seq(Map()),
      new FailingNextIterable(Map())
    )

    val pipe = TransactionApplySlottedPipe(lhs, rhs, literal(1), OnErrorContinue, Set.empty, slots.get("status"))()

    withQueryState(IMPLICIT) { state =>
      state.setExecutionContextFactory(SlottedCypherRowFactory(slots, slots.size()))
      pipe.rowFactory = SlottedCypherRowFactory(slots, slots.size())

      val result = pipe.createResults(state)

      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertFailedBatch(result.next())
      result.hasNext shouldBe true
      assertSuccessfulBatch(result.next())
      result.hasNext shouldBe true
      assertFailedBatch(result.next())

      result.hasNext shouldBe false

      rhs.allWasClosed shouldBe true
      lhs.allWasClosed shouldBe true
    }
  }

  private def assertSuccessfulBatch(row: CypherRow): Unit = assertBatchStatus(row, committed = true, started = true)
  private def assertFailedBatch(row: CypherRow): Unit = assertBatchStatus(row, committed = false, started = true)
  private def assertSkippedBatch(row: CypherRow): Unit = assertBatchStatus(row, committed = false, started = false)

  private def assertBatchStatus(row: CypherRow, committed: Boolean, started: Boolean): Unit = {
    row.getRefAt(0) match {
      case status: MapValue =>
        status.get("committed") shouldBe Values.booleanValue(committed)
        status.get("started") shouldBe Values.booleanValue(started)
        if (started && !committed) {
          status.get("errorMessage").asInstanceOf[TextValue].stringValue() shouldBe "Hello, I fail for you"
        } else {
          status.get("errorMessage") shouldBe Values.NO_VALUE
        }
      case other => throw new IllegalArgumentException(s"Unexpected type $other")
    }
  }
}

class FailingNextIterable[T](succeeds: T*) extends Iterable[T] {

  override def iterator: Iterator[T] = new Iterator[T] {
    private val iter = succeeds.iterator
    override def hasNext: Boolean = true

    override def next(): T = {
      if (iter.hasNext) iter.next()
      else throw new TransactionApplyTestException("Hello, I fail for you", Status.Classification.TransientError)
    }
  }
}

class TransactionApplyTestException(message: String, classification: Status.Classification)
    extends RuntimeException(message) with HasStatus {

  override val status: Status = new Status() {

    override val code: Status.Code = {
      val mockCode = mock[Status.Code]
      when(mockCode.classification()).thenReturn(classification)
      mockCode
    }
  }
}

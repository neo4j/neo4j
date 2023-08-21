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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureAccessMode
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EagerReadWriteCallMode
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toIntValue
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toStringValue
import org.neo4j.cypher.internal.runtime.LazyReadOnlyCallMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

import java.util.UUID

class ProcedureCallPipeTest
    extends CypherFunSuite
    with PipeTestSupport
    with ImplicitDummyPos {

  private val ID = 42
  private val procedureName: QualifiedName = QualifiedName(List.empty, "foo")

  private val signature: ProcedureSignature = ProcedureSignature(
    procedureName,
    IndexedSeq.empty,
    Some(IndexedSeq(FieldSignature("foo", CTAny))),
    None,
    ProcedureReadOnlyAccess,
    id = ID
  )

  test("should execute read-only procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator)

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = LazyReadOnlyCallMode,
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> (("r", "r")))
    )()

    val qtx = fakeQueryContext(ID, resultsTransformer, ProcedureReadOnlyAccess)

    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      CypherRow.from("a" -> 1, "r" -> "take 1/1"),
      CypherRow.from("a" -> 2, "r" -> "take 1/2"),
      CypherRow.from("a" -> 2, "r" -> "take 2/2")
    ))
  }

  test("should execute read-write procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator)

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = EagerReadWriteCallMode,
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> (("r", "r")))
    )()

    val qtx = fakeQueryContext(ID, resultsTransformer, ProcedureReadWriteAccess)
    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      CypherRow.from("a" -> 1, "r" -> "take 1/1"),
      CypherRow.from("a" -> 2, "r" -> "take 1/2"),
      CypherRow.from("a" -> 2, "r" -> "take 2/2")
    ))
  }

  test("should execute void procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator)

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = EagerReadWriteCallMode,
      argExprs = Seq(Variable("a")),
      rowProcessing = PassThroughRow,
      resultSymbols = Seq.empty,
      resultIndices = Seq.empty
    )()

    val qtx = fakeQueryContext(ID, _ => Iterator.empty, ProcedureReadWriteAccess)
    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      CypherRow.from("a" -> 1),
      CypherRow.from("a" -> 2)
    ))
  }

  private def resultsTransformer(args: Seq[AnyValue]): Iterator[Array[AnyValue]] = {
    val count = args.head.asInstanceOf[NumberValue].longValue().intValue()
    1.to(count).map { i =>
      Array[AnyValue](Values.stringValue(s"take $i/$count"))
    }

  }.toIterator

  private def fakeQueryContext(
    id: Int,
    result: Seq[AnyValue] => Iterator[Array[AnyValue]],
    expectedAccessMode: ProcedureAccessMode
  ): QueryContext = {

    def doIt(id: Int, args: Array[AnyValue]): Iterator[Array[AnyValue]] = {
      id should equal(ID)
      args.length should be(1)
      result(args)
    }

    val transactionalContext = mock[QueryTransactionalContext]
    val databaseID = DatabaseIdFactory.from("neo4j", UUID.randomUUID())

    Mockito.when(transactionalContext.databaseId).thenReturn(databaseID)

    val queryContext = mock[QueryContext]
    Mockito.when(
      queryContext.callReadOnlyProcedure(any[Int](), any[Array[AnyValue]](), any[ProcedureCallContext])
    ).thenAnswer(
      new Answer[Iterator[Array[AnyValue]]] {
        override def answer(invocationOnMock: InvocationOnMock): Iterator[Array[AnyValue]] = {
          expectedAccessMode should equal(ProcedureReadOnlyAccess)
          doIt(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1))
        }
      }
    )

    Mockito.when(
      queryContext.callReadWriteProcedure(any[Int](), any[Array[AnyValue]](), any[ProcedureCallContext])
    ).thenAnswer(
      new Answer[Iterator[Array[AnyValue]]] {
        override def answer(invocationOnMock: InvocationOnMock): Iterator[Array[AnyValue]] = {
          expectedAccessMode should equal(ProcedureReadWriteAccess)
          doIt(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1))
        }
      }
    )
    Mockito.when(queryContext.transactionalContext).thenReturn(transactionalContext)

    Mockito.when(queryContext.asObject(any[AnyValue]())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocationOnMock: InvocationOnMock): AnyRef =
          invocationOnMock.getArgument[AnyValue](0) match {
            case i: IntValue  => Int.box(i.value())
            case l: LongValue => Long.box(l.value())
            case _            => throw new IllegalStateException()
          }
      }
    )

    queryContext
  }
}

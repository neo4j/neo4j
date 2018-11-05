/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{EagerReadWriteCallMode, LazyReadOnlyCallMode, QueryContext}
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{IntValue, LongValue}
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.scalatest.mock.MockitoSugar

class ProcedureCallPipeTest
  extends CypherFunSuite
    with PipeTestSupport
    with ImplicitDummyPos
    with MockitoSugar {

  val ID = 42
  val procedureName = QualifiedName(List.empty, "foo")
  val signature = ProcedureSignature(procedureName, IndexedSeq.empty, Some(IndexedSeq(FieldSignature("foo", CTAny))),
                                     None, ProcedureReadOnlyAccess(Array.empty), id = Some(ID))
  val emptyStringArray = Array.empty[String]

  test("should execute read-only procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber) {}

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = LazyReadOnlyCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> "r")
    )()

    val qtx = fakeQueryContext(ID, resultsTransformer, ProcedureReadOnlyAccess(emptyStringArray))

    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      ExecutionContext.from("a" ->1, "r" -> "take 1/1"),
      ExecutionContext.from("a" ->2, "r" -> "take 1/2"),
      ExecutionContext.from("a" ->2, "r" -> "take 2/2")
    ))
  }

  test("should execute read-write procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = EagerReadWriteCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> "r")
    )()

    val qtx = fakeQueryContext(ID, resultsTransformer, ProcedureReadWriteAccess(emptyStringArray))
    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      ExecutionContext.from("a" -> 1, "r" -> "take 1/1"),
      ExecutionContext.from("a" -> 2, "r" -> "take 1/2"),
      ExecutionContext.from("a" -> 2, "r" -> "take 2/2")
    ))
  }

  test("should execute void procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val pipe = ProcedureCallPipe(
      source = lhs,
      signature = signature,
      callMode = EagerReadWriteCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = PassThroughRow,
      resultSymbols = Seq.empty,
      resultIndices = Seq.empty
    )()

    val qtx = fakeQueryContext(ID, _ => Iterator.empty, ProcedureReadWriteAccess(emptyStringArray))
    pipe.createResults(QueryStateHelper.emptyWith(query = qtx)).toList should equal(List(
      ExecutionContext.from("a" -> 1),
      ExecutionContext.from("a" -> 2)
    ))
  }

  private def resultsTransformer(args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val count = args.head.asInstanceOf[Number].intValue()
    1.to(count).map { i =>
      Array[AnyRef](s"take $i/$count")
    }

  }.toIterator

  private def fakeQueryContext(id: Int,
                               result: Seq[Any] => Iterator[Array[AnyRef]],
                               expectedAccessMode: ProcedureAccessMode): QueryContext = {

    def doIt(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
      id should equal(ID)
      args.length should be(1)
      result(args)
    }

    val queryContext = mock[QueryContext]
    Mockito.when(queryContext.callReadOnlyProcedure(any[Int](), any[Seq[Any]](), any[Array[String]]())).thenAnswer(
      new Answer[Iterator[Array[AnyRef]]] {
        override def answer(invocationOnMock: InvocationOnMock): Iterator[Array[AnyRef]] = {
          expectedAccessMode should equal(ProcedureReadOnlyAccess(emptyStringArray))
          doIt(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1), invocationOnMock.getArgument(2))
        }
      }
    )

    Mockito.when(queryContext.callReadWriteProcedure(any[Int](), any[Seq[Any]](), any[Array[String]]())).thenAnswer(
      new Answer[Iterator[Array[AnyRef]]] {
        override def answer(invocationOnMock: InvocationOnMock): Iterator[Array[AnyRef]] = {
          expectedAccessMode should equal(ProcedureReadWriteAccess(emptyStringArray))
          doIt(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1), invocationOnMock.getArgument(2))
        }
      }
    )

    Mockito.when(queryContext.asObject(any[AnyValue]())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocationOnMock: InvocationOnMock): AnyRef =
          invocationOnMock.getArgument[AnyValue](0) match {
            case i: IntValue => Int.box(i.value())
            case l: LongValue => Long.box(l.value())
            case _ => throw new IllegalStateException()
          }
      }
    )

    queryContext
  }
}

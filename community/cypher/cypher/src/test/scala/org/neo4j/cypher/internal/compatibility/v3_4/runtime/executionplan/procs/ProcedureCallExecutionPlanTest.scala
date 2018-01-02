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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.{CloseableResource, NormalMode, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.values.storable.LongValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class ProcedureCallExecutionPlanTest extends CypherFunSuite {

  private val converters = new ExpressionConverters(CommunityExpressionConverter)

  test("should be able to call procedure with single argument") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature, Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          notifications = Set.empty, converters)

    // When
    val res = proc.run(ctx, NormalMode, EMPTY_MAP)

    // Then
    res.toList should equal(List(Map("b" -> 84)))
  }

  test("should eagerize write procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(writeSignature,
                                          Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          notifications = Set.empty, converters)

    // When
    proc.run(ctx, NormalMode, EMPTY_MAP)

    // Then without touching the result, it should have been spooled out
    iteratorExhausted should equal(true)
  }

  test("should not eagerize read procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature,
                                          Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          notifications = Set.empty, converters)

    // When
    proc.run(ctx, NormalMode, EMPTY_MAP)

    // Then without touching the result, the Kernel iterator should not be touched
    iteratorExhausted should equal(false)
  }

  override protected def beforeEach() = iteratorExhausted = false

  def add(lhs: Expression, rhs: Expression): Expression = Add(lhs, rhs)(pos)

  def int(i: Int): Expression = SignedDecimalIntegerLiteral(i.toString)(pos)

  def string(s: String): Expression = StringLiteral(s)(pos)

  private val readSignature = ProcedureSignature(
    QualifiedName(IndexedSeq.empty, "foo"),
    IndexedSeq(FieldSignature("a", CTInteger)),
    Some(IndexedSeq(FieldSignature("b", CTInteger))),
    None,
    ProcedureReadOnlyAccess(Array.empty)
  )

  private val writeSignature = ProcedureSignature(
    QualifiedName(Seq.empty, "foo"),
    IndexedSeq(FieldSignature("a", CTInteger)),
    Some(IndexedSeq(FieldSignature("b", CTInteger))),
    None,
    ProcedureReadWriteAccess(Array.empty)
  )

  private val pos = DummyPosition(-1)
  val ctx = mock[QueryContext]
  when(ctx.resources).thenReturn(mock[CloseableResource])
  var iteratorExhausted = false

  val procedureResult = new Answer[Iterator[Array[AnyRef]]] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val input: Seq[AnyRef] = invocationOnMock.getArgument(1)
      new Iterator[Array[AnyRef]] {
        override def hasNext = !iteratorExhausted

        override def next() = if (hasNext) {
          iteratorExhausted = true
          input.toArray
        } else throw new IllegalStateException("Iterator exhausted")
      }
    }
  }

  when(ctx.transactionalContext).thenReturn(mock[QueryTransactionalContext])
  when(ctx.callReadOnlyProcedure(any[QualifiedName], any[Seq[Any]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.callReadWriteProcedure(any[QualifiedName], any[Seq[Any]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.asObject(any[LongValue])).thenAnswer(new Answer[Long]() {
    override def answer(invocationOnMock: InvocationOnMock): Long = invocationOnMock.getArgument(0).asInstanceOf[LongValue].value()
  })
}

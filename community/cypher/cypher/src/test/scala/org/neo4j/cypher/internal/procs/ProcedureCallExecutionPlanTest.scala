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
package org.neo4j.cypher.internal.procs

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.{NoInput, QueryContext, QueryTransactionalContext, ResourceManager}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.procs.{QualifiedName => KernelQualifiedName}
import org.neo4j.internal.kernel.api.{CursorFactory, Procedures}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.JavaConverters._

class ProcedureCallExecutionPlanTest extends CypherFunSuite {

  private val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))
  private val idGen = new SequentialIdGen()

  test("should be able to call procedure with single argument") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature, Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          converters, idGen.id())

    // When
    val res = proc.run(ctx, doProfile = false, EMPTY_MAP, prePopulateResults = false, NoInput)

    // Then
    toList(res) should equal(List(Map("b" -> 84)))
  }

  test("should eagerize write procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(writeSignature,
                                          Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          converters, idGen.id())

    // When
    proc.run(ctx, doProfile = false, EMPTY_MAP, prePopulateResults = false, NoInput)

    // Then without touching the result, it should have been spooled out
    iteratorExhausted should equal(true)
  }

  test("should not eagerize readOperationsInNewTransaction procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature,
                                          Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b"),
                                          converters, idGen.id())

    // When
    proc.run(ctx, doProfile = false, EMPTY_MAP, prePopulateResults = false, NoInput)

    // Then without touching the result, the Kernel iterator should not be touched
    iteratorExhausted should equal(false)
  }

  override protected def beforeEach(): Unit = iteratorExhausted = false

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

  private def toList(res: RuntimeResult): List[Map[String, AnyRef]] =
    res.asIterator().asScala.map(_.asScala.toMap).toList

  private val pos = DummyPosition(-1)
  private val ctx = mock[QueryContext](org.mockito.Mockito.RETURNS_DEEP_STUBS)
  when(ctx.resources).thenReturn(mock[ResourceManager])
  private var iteratorExhausted = false

  private val procedureResult = new Answer[Iterator[Array[AnyValue]]] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val input: Seq[AnyValue] = invocationOnMock.getArgument(1)
      new Iterator[Array[AnyValue]] {
        override def hasNext: Boolean = !iteratorExhausted

        override def next(): Array[AnyValue] = if (hasNext) {
          iteratorExhausted = true
          input.toArray
        } else throw new IllegalStateException("Iterator exhausted")
      }
    }
  }

  private val procs = mock[Procedures]
  private val transactionalContext: QueryTransactionalContext = mock[QueryTransactionalContext]
  when(ctx.transactionalContext).thenReturn(transactionalContext)
  when(transactionalContext.cursors).thenReturn(mock[CursorFactory])
  when(ctx.callReadOnlyProcedure(anyInt, any[Seq[AnyValue]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.callReadOnlyProcedure(any[KernelQualifiedName], any[Seq[AnyValue]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.callReadWriteProcedure(anyInt, any[Seq[AnyValue]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.callReadWriteProcedure(any[KernelQualifiedName], any[Seq[AnyValue]], any[Array[String]])).thenAnswer(procedureResult)
  when(ctx.asObject(any[LongValue])).thenAnswer(new Answer[Long]() {
    override def answer(invocationOnMock: InvocationOnMock): Long = invocationOnMock.getArgument(0).asInstanceOf[LongValue].value()
  })
}

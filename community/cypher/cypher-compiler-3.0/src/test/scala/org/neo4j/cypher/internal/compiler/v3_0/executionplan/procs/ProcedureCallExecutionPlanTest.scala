/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_0.NormalMode
import org.neo4j.cypher.internal.compiler.v3_0.spi.{QueryContext, _}
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.spi._
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.{DummyPosition, spi => frontend, symbols}
import org.neo4j.cypher.internal.compiler.v3_0.spi.ProcedureName

class ProcedureCallExecutionPlanTest extends CypherFunSuite {

  test("should be able to call procedure with single argument") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature,
      Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b")
    )

    // When
    val res = proc.run(ctx, NormalMode, Map.empty)

    // Then
    res.toList should equal(List(Map("b" -> 84)))
  }

  test("should eagerize write procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(writeSignature,
      Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b")
    )

    // When
    proc.run(ctx, NormalMode, Map.empty)

    // Then without touching the result, it should have been spooled out
    iteratorExhausted should equal(true)
  }

  test("should not eagerize read procedure") {
    // Given
    val proc = ProcedureCallExecutionPlan(readSignature,
      Seq(add(int(42), int(42))), Seq("b" -> CTInteger), Seq(0 -> "b")
    )

    // When
    proc.run(ctx, NormalMode, Map.empty)

    // Then without touching the result, the Kernel iterator should not be touched
    iteratorExhausted should equal(false)
  }

  override protected def beforeEach() = iteratorExhausted = false

  def add(lhs: Expression, rhs: Expression): Expression = Add(lhs, rhs)(pos)

  def int(i: Int): Expression = SignedDecimalIntegerLiteral(i.toString)(pos)

  def string(s: String): Expression = StringLiteral(s)(pos)

  private val readSignature = frontend.ProcedureSignature(
    QualifiedProcedureName(Seq.empty, "foo"),
    Seq(frontend.FieldSignature("a", symbols.CTInteger)),
    Seq(frontend.FieldSignature("b", symbols.CTInteger)),
    ProcedureReadOnlyAccess
  )

  private val writeSignature =  frontend.ProcedureSignature(
    QualifiedProcedureName(Seq.empty, "foo"),
    Seq(frontend.FieldSignature("a", symbols.CTInteger)),
    Seq(frontend.FieldSignature("b", symbols.CTInteger)),
    ProcedureReadWriteAccess
  )

  private val pos = DummyPosition(-1)
  val ctx = mock[QueryContext]
  var iteratorExhausted = false

  val procedureResult = new Answer[Iterator[Array[AnyRef]]] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val input = invocationOnMock.getArguments()(1).asInstanceOf[Seq[AnyRef]]
      new Iterator[Array[AnyRef]] {
        override def hasNext = !iteratorExhausted
        override def next() = if(hasNext) {
          iteratorExhausted = true
          input.toArray
        } else throw new IllegalStateException("Iterator exhausted")
      }
    }
  }

  when(ctx.transactionalContext).thenReturn(mock[QueryTransactionalContext])
  when(ctx.callReadOnlyProcedure(any[ProcedureName], any[Seq[Any]])).thenAnswer(procedureResult)
  when(ctx.callReadWriteProcedure(any[ProcedureName], any[Seq[Any]])).thenAnswer(procedureResult)
}

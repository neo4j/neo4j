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
import org.neo4j.cypher.internal.compiler.v3_0.spi.{FieldSignature, ProcedureName, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Add, Expression, SignedDecimalIntegerLiteral, StringLiteral}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.{DummyPosition, InvalidArgumentException, symbols}

class CallProcedureExecutionPlanTest extends CypherFunSuite {

  test("should be able to call procedure with single argument") {
    // Given
    val signature = ProcedureSignature(
      ProcedureName(Seq.empty, "foo"), Seq(FieldSignature("a", symbols.CTInteger)),
      Seq(FieldSignature("b", symbols.CTInteger)))

    val proc = CallProcedureExecutionPlan(signature, Some(Seq(add(int(42), int(42)))))

    // When
    val res = proc.run(ctx, NormalMode, Map.empty)

    // Then
    res.toList should equal(List(Map("b" -> 84)))
  }

  test("should be able to call procedure with single argument using parameters") {
    // Given
    val signature = ProcedureSignature(
      ProcedureName(Seq.empty, "foo"), Seq(FieldSignature("a", symbols.CTInteger)),
      Seq(FieldSignature("b", symbols.CTInteger)))

    val proc = CallProcedureExecutionPlan(signature, None)

    // When
    val res = proc.run(ctx, NormalMode, Map("a" -> 84))

    // Then
    res.toList should equal(List(Map("b" -> 84)))
  }

  test("should fail if parameter is missing") {
    // Given
    val signature = ProcedureSignature(
      ProcedureName(Seq.empty, "foo"), Seq(FieldSignature("a", symbols.CTInteger), FieldSignature("b", symbols.CTInteger)),
      Seq(FieldSignature("c", symbols.CTInteger)))

    // When
    val proc = CallProcedureExecutionPlan(signature, None)

    // Then
    an [InvalidArgumentException] should be thrownBy proc.run(ctx, NormalMode, Map("a" -> 84))
  }


  def add(lhs: Expression, rhs: Expression): Expression = Add(lhs, rhs)(pos)

  def int(i: Int): Expression = SignedDecimalIntegerLiteral(i.toString)(pos)

  def string(s: String): Expression = StringLiteral(s)(pos)

  private val pos = DummyPosition(-1)
  val ctx = mock[QueryContext]
  when(ctx.callReadOnlyProcedure(any[ProcedureSignature], any[Seq[AnyRef]])).thenAnswer(new Answer[Iterator[Array[AnyRef]]] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val input = invocationOnMock.getArguments()(1).asInstanceOf[Seq[AnyRef]]
      Iterator.single(input.toArray)
    }
  })
}

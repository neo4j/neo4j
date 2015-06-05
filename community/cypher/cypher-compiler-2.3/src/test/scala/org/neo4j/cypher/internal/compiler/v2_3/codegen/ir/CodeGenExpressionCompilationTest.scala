/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions.CodeGenExpression.{add, parameter, sub}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.scalatest._

class CodeGenExpressionCompilationTest extends CypherFunSuite with Matchers with CodeGenSugar {
  private val traceIds = Map("X" -> null, "id" -> null)

  // addition

  {
    // literal + literal
    def adding(lhs: CodeGenExpression, rhs: CodeGenExpression) = {
      val addition = add(lhs, rhs)
      evaluate(
        Seq(Project("X", Seq.empty, AcceptVisitor("id", Map("result" -> addition)))), operatorIds = traceIds)
    }

    verifyAddition(adding, new SimpleOperands[CodeGenExpression]("literal") {
      override def value(value: Object) = literal(value)
    })
  }

  {
    // parameter + parameter
    val addition: CodeGenExpression = add(parameter("lhs"), parameter("rhs"))
    val instructions = Seq(Project("X", Seq(addition), AcceptVisitor("id", Map("result" -> addition))))

    def adding(lhs: Object, rhs: Object) = evaluate(instructions, params = Map("lhs" -> lhs, "rhs" -> rhs), operatorIds = traceIds)

    verifyAddition(adding, new SimpleOperands[Object]("parameter") {
      override def value(value: Object) = value
    })
  }

  {
    // literal + parameter
    def adding(lhs: CodeGenExpression, rhs: Object) = {
      val addition: CodeGenExpression = add(lhs, parameter("rhs"))
      val instructions = Seq(Project("X", Seq(addition), AcceptVisitor("id", Map("result" -> addition))))
      evaluate(instructions, params = Map("rhs" -> rhs), operatorIds = traceIds)

    }

    verifyAddition(adding, new Operands[CodeGenExpression, Object]("literal", "parameter") {
      override def lhs(value: Object) = literal(value)
      override def rhs(value: Object) = value
    })
  }

  {
    // parameter + literal
    def adding(lhs: Object, rhs: CodeGenExpression) = {
      val addition: CodeGenExpression = add(parameter("lhs"), rhs)
      val instructions = Seq(Project("X", Seq(addition), AcceptVisitor("id", Map("result" -> addition))))
      evaluate(instructions, params = Map("lhs" -> lhs), operatorIds = traceIds)
    }

    verifyAddition(adding, new Operands[Object, CodeGenExpression]("parameter", "literal") {
      override def lhs(value: Object) = value
      override def rhs(value: Object) = literal(value)
    })
  }

  // subtraction

  {
    // literal - literal
    def subtracting(lhs: CodeGenExpression, rhs: CodeGenExpression) = {
      val subtraction = sub(lhs, rhs)
      evaluate(
        Seq(Project("X", Seq.empty, AcceptVisitor("id", Map("result" -> subtraction)))), operatorIds = traceIds)
    }

    verifySubtraction(subtracting, new SimpleOperands[CodeGenExpression]("literal") {
      override def value(value: Object) = literal(value)
    })
  }

  {
    // parameter - parameter
    val subtraction: CodeGenExpression = sub(parameter("lhs"), parameter("rhs"))
    val instructions = Seq(Project("X", Seq(subtraction), AcceptVisitor("id", Map("result" -> subtraction))))
    def subtracting(lhs: Object, rhs: Object) = evaluate(instructions, params = Map("lhs" -> lhs, "rhs" -> rhs), operatorIds = traceIds)

    verifySubtraction(subtracting, new SimpleOperands[Object]("parameter") {
      override def value(value: Object) = value
    })
  }

  {
    // literal - parameter
    def subtracting(lhs: CodeGenExpression, rhs: Object) = {
      val subtraction: CodeGenExpression = sub(lhs, parameter("rhs"))
      val instructions = Seq(Project("X", Seq(subtraction), AcceptVisitor("id", Map("result" -> subtraction))))
      evaluate(instructions, params = Map("rhs" -> rhs), operatorIds = traceIds)
    }

    verifySubtraction(subtracting, new Operands[CodeGenExpression, Object]("literal", "parameter") {
      override def lhs(value: Object) = literal(value)
      override def rhs(value: Object) = value
    })
  }

  {
    // parameter - literal
    def subtracting(lhs: Object, rhs: CodeGenExpression) = {
      val subtraction: CodeGenExpression = sub(parameter("lhs"), rhs)
      val instructions = Seq(Project("X", Seq(subtraction), AcceptVisitor("id", Map("result" -> subtraction))))
      evaluate(instructions, params = Map("lhs" -> lhs), operatorIds = traceIds)
    }

    verifySubtraction(subtracting, new Operands[AnyRef, CodeGenExpression]("parameter", "literal") {
      override def lhs(value: Object) = value
      override def rhs(value: Object) = literal(value)
    })
  }

  type Operator[Lhs, Rhs] = (Lhs, Rhs) => List[Map[String, Any]]

  private def verifyAddition[Lhs, Rhs](add: Operator[Lhs, Rhs], value: Operands[Lhs, Rhs]) = {
    verify(add, java.lang.Long.valueOf(7), "+", java.lang.Long.valueOf(9), value, java.lang.Long.valueOf(16))
    verify(add, "abc", "+", java.lang.Long.valueOf(7), value, "abc7")
    verify(add, java.lang.Long.valueOf(9), "+", "abc", value, "9abc")
    verify(add, java.lang.Double.valueOf(3.14), "+", "abc", value, "3.14abc")
    verify(add, "abc", "+", java.lang.Double.valueOf(3.14), value, "abc3.14")
    verify(add, java.lang.Long.valueOf(7), "+", java.lang.Double.valueOf(3.14), value, 10.14)
    verify(add, java.lang.Double.valueOf(11.6), "+", java.lang.Long.valueOf(3), value, java.lang.Double.valueOf(14.6))
    verify(add, java.lang.Double.valueOf(2.5), "+", java.lang.Double.valueOf(4.5), value, java.lang.Double.valueOf(7.0))
    verify(add, java.lang.Long.valueOf(Long.MaxValue), "+", java.lang.Long.valueOf(Long.MinValue), value, java.lang.Long.valueOf(-1))
  }

  private def verifySubtraction[Lhs, Rhs](sub: Operator[Lhs, Rhs], value: Operands[Lhs, Rhs]) = {
    verify(sub, java.lang.Long.valueOf(9), "-", java.lang.Long.valueOf(7), value, java.lang.Long.valueOf(2))
    verify(sub, java.lang.Long.valueOf(Long.MaxValue), "-", java.lang.Long.valueOf(Int.MaxValue), value, java.lang.Long.valueOf(Long.MaxValue - Int.MaxValue))
    verify(sub, java.lang.Double.valueOf(3.25), "-", java.lang.Long.valueOf(3), value, java.lang.Double.valueOf(0.25))
    verify(sub, java.lang.Double.valueOf(3.21), "-", java.lang.Double.valueOf(1.23), value, java.lang.Double.valueOf(1.98))
    verify(sub, java.lang.Long.valueOf(-1), "-", java.lang.Long.valueOf(-2), value, java.lang.Long.valueOf(1))
    verify(sub, java.lang.Double.valueOf(-1.25), "-", java.lang.Double.valueOf(-2.5), value, java.lang.Double.valueOf(1.25))
  }

  abstract class Operands[Lhs, Rhs](val left: String, val right: String) {
    def lhs(value: Object): Lhs
    def rhs(value: Object): Rhs
  }

  abstract class SimpleOperands[Value](v: String) extends Operands[Value, Value](v, v) {
    override def lhs(v: Object) = value(v)
    override def rhs(v: Object) = value(v)
    def value(value: Object): Value
  }

  def verify[Lhs, Rhs](operator: Operator[Lhs, Rhs], lhs: Object, op: String, rhs: Object, value: Operands[Lhs, Rhs], result: Any) {
    test(s"${lhs.getClass.getSimpleName} ${value.left} ($lhs) $op ${rhs.getClass.getSimpleName} ${value.right} ($rhs)") {
      operator(value.lhs(lhs), value.rhs(rhs)) shouldEqual List(Map("result" -> result))
    }
  }

  def literal(value: Any): CodeGenExpression = value match {
    case v: Int => CodeGenExpression.literal(v)
    case v: Long => CodeGenExpression.literal(v)
    case v: Double => CodeGenExpression.literal(v)
    case v: String => CodeGenExpression.literal(v)
  }
}

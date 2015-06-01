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
import CodeGenExpression.{add, parameter, sub}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.scalatest._

class CodeGenExpressionCompilationTest extends CypherFunSuite with Matchers with CodeGenSugar {
  // addition

  { // literal + literal
    def adding(lhs: CodeGenExpression, rhs: CodeGenExpression) = {
    val addition = add(lhs, rhs)
    evaluate(
      Project("X", Seq.empty, AcceptVisitor("id", Map("result" -> addition))))
  }


    verifyAddition(adding, new SimpleOperands[CodeGenExpression]("literal") {
      override def value(value: Any) = literal(value)
    })
  }

  { // parameter + parameter
    val addition: CodeGenExpression = add(parameter("lhs"), parameter("rhs"))
    val clazz = compile(Project("X", Seq(addition), AcceptVisitor("id", Map("result" -> addition))))

    def adding(lhs: Any, rhs: Any) = evaluate(newInstance(clazz, params = Map("lhs" -> lhs, "rhs" -> rhs)))

    verifyAddition(adding, new SimpleOperands[Any]("parameter") {
      override def value(value: Any) = value
    })
  }

  { // literal + parameter
    def adding(lhs: CodeGenExpression, rhs: Any) = {
      val addition: CodeGenExpression = add(lhs, parameter("rhs"))
      evaluate(newInstance(compile(Project("X", Seq(addition), AcceptVisitor("id", Map("result" -> addition)))), params = Map("rhs" -> rhs)))

    }

    verifyAddition(adding, new Operands[CodeGenExpression, Any]("literal", "parameter") {
      override def lhs(value: Any) = literal(value)

      override def rhs(value: Any) = value
    })
  }

  { // parameter + literal
    def adding(lhs: Any, rhs: CodeGenExpression) = {
      val addition: CodeGenExpression = add(parameter("lhs"), rhs)
      evaluate(newInstance(compile(Project("X", Seq(addition),
        AcceptVisitor("id", Map("result" -> addition)))), params = Map("lhs" -> lhs)))
    }

    verifyAddition(adding, new Operands[Any, CodeGenExpression]("parameter", "literal") {
      override def lhs(value: Any) = value

      override def rhs(value: Any) = literal(value)
    })
  }

  // subtraction

  { // literal - literal
    def subtracting(lhs: CodeGenExpression, rhs: CodeGenExpression) = {
    val subtraction = sub(lhs, rhs)
    evaluate(
      Project("X", Seq.empty, AcceptVisitor("id", Map("result" -> subtraction))))
  }

    verifySubtraction(subtracting, new SimpleOperands[CodeGenExpression]("literal") {
      override def value(value: Any) = literal(value)
    })
  }

  { // parameter - parameter
    val subtraction: CodeGenExpression = sub(parameter("lhs"), parameter("rhs"))
    val clazz = compile(Project("X", Seq(subtraction), AcceptVisitor("id", Map("result" -> subtraction))))
    def subtracting(lhs: Any, rhs: Any) = evaluate(newInstance(clazz, params = Map("lhs" -> lhs, "rhs" -> rhs)))

    verifySubtraction(subtracting, new SimpleOperands[Any]("parameter") {
      override def value(value: Any) = value
    })
  }

  { // literal - parameter
    def subtracting(lhs: CodeGenExpression, rhs: Any) = {
      val subtraction: CodeGenExpression = sub(lhs, parameter("rhs"))
      evaluate(newInstance(compile(Project("X", Seq(subtraction),
        AcceptVisitor("id", Map("result" ->  subtraction)))), params = Map("rhs" -> rhs)))

    }

    verifySubtraction(subtracting, new Operands[CodeGenExpression, Any]("literal", "parameter") {
      override def lhs(value: Any) = literal(value)

      override def rhs(value: Any) = value
    })
  }

  { // parameter - literal
    def subtracting(lhs: Any, rhs: CodeGenExpression) = {
      val subtraction: CodeGenExpression = sub(parameter("lhs"), rhs)
      evaluate(newInstance(compile(Project("X", Seq(subtraction),
        AcceptVisitor("id", Map("result" ->  subtraction)))), params = Map("lhs" -> lhs)))
    }

    verifySubtraction(subtracting, new Operands[Any, CodeGenExpression]("parameter", "literal") {
      override def lhs(value: Any) = value

      override def rhs(value: Any) = literal(value)
    })
  }

  type Operator[Lhs, Rhs] = (Lhs, Rhs) => List[Map[String, Any]]

  private def verifyAddition[Lhs, Rhs](add: Operator[Lhs, Rhs], value: Operands[Lhs, Rhs]) = {
    verify(add, 7, "+", 9, value, 16)
    verify(add, "abc", "+", 7, value, "abc7")
    verify(add, 9, "+", "abc", value, "9abc")
    verify(add, 3.14, "+", "abc", value, "3.14abc")
    verify(add, "abc", "+", 3.14, value, "abc3.14")
    verify(add, 7, "+", 3.14, value, 10.14)
    verify(add, 11.6, "+", 3, value, 14.6)
    verify(add, 2.5, "+", 4.5, value, 7.0)
    verify(add, Long.MaxValue, "+", Long.MinValue, value, -1)
  }

  private def verifySubtraction[Lhs, Rhs](sub: Operator[Lhs, Rhs], value: Operands[Lhs, Rhs]) = {
    verify(sub, 9, "-", 7, value, 2)
    verify(sub, Long.MaxValue, "-", Int.MaxValue, value, Long.MaxValue - Int.MaxValue)
    verify(sub, 3.25, "-", 3, value, 0.25)
    verify(sub, 3.21, "-", 1.23, value, 1.98)
    verify(sub, -1, "-", -2, value, 1)
    verify(sub, -1.25, "-", -2.5, value, 1.25)
  }

  abstract class Operands[Lhs, Rhs](val left: String, val right: String) {
    def lhs(value: Any): Lhs
    def rhs(value: Any): Rhs
  }

  abstract class SimpleOperands[Value](v: String) extends Operands[Value, Value](v, v) {
    override def lhs(v: Any) = value(v)
    override def rhs(v: Any) = value(v)
    def value(value: Any): Value
  }

  def verify[Lhs, Rhs](operator: Operator[Lhs, Rhs], lhs: Any, op: String, rhs: Any, value: Operands[Lhs, Rhs], result: Any) {
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

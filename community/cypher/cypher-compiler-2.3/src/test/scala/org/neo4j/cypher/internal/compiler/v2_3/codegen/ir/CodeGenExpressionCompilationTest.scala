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

import org.neo4j.cypher.internal.compiler.v2_3.CypherTypeException
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.scalatest._

class CodeGenExpressionCompilationTest extends CypherFunSuite with Matchers with CodeGenSugar {

  private val traceIds = Map("X" -> null, "id" -> null)

  case class Operation[E <: Exception](name: String,
                                       execute: (CodeGenExpression, CodeGenExpression) => CodeGenExpression,
                                       data: Seq[(Any, Any, Either[Any, Class[E]])])

  val additionOperation = Operation("Addition", Addition.apply, Seq(
    (7, 9, Left(16)),
    ("abc", 7, Left("abc7")),
    (9, "abc", Left("9abc")),
    (3.14, "abc", Left("3.14abc")),
    ("abc", 3.14, Left("abc3.14")),
    (7, 3.14, Left(10.14)),
    (11.6, 3, Left(14.6)),
    (2.5, 4.5, Left(7.0)),
    (Long.MaxValue, Long.MinValue, Left(-1)),
    (42, null, Left(null)),
    (null, 42, Left(null)),
    (true, 3, Right(classOf[CypherTypeException])))
  )

  val subtractionOperation = Operation("Subtraction", Subtraction.apply, Seq(
    (9, 7, Left(2)),
    (Long.MaxValue, Int.MaxValue, Left(Long.MaxValue - Int.MaxValue)),
    (3.25, 3, Left(0.25)),
    (3.21, 1.23, Left(1.98)),
    (-1, -2, Left(1)),
    (-1.25, -2.5, Left(1.25)),
    (2, null, Left(null)),
    (null, 2, Left(null)),
    (false, 3, Right(classOf[CypherTypeException])))
  )

  val operationsToTest = Seq(additionOperation, subtractionOperation)

  for (Operation(name, apply, data) <- operationsToTest) {

    val inputs = data.map {
      case (x, y, r) => (asAnyRef(x), asAnyRef(y), asAnyRef(r))
    }

    def project(lhs: CodeGenExpression, rhs: CodeGenExpression) =
      Seq(Project("X", Seq.empty, AcceptVisitor("id", Map("result" -> apply(lhs, rhs)))))

    inputs.foreach {
      case (lhs, rhs, expected) =>

        def verify(result: => AnyRef) = expected match {
          case Left(x) =>
            result should equal(List(Map("result" -> x)))

          case Right(x) =>
            val error = intercept[Exception](result)
            withClue(error)(error.getClass should equal(x))
        }

        test(s"$name: literal($lhs) & literal($rhs)") {
          verify(evaluate(project(Literal(lhs), Literal(rhs)), operatorIds = traceIds,
                          params = Map.empty))
        }

        test(s"$name: parameter($lhs) & parameter($rhs)") {
          verify(evaluate(project(Parameter("lhs"), Parameter("rhs")), operatorIds = traceIds,
                          params = Map("lhs" -> lhs, "rhs" -> rhs)))
        }

        test(s"$name: literal($lhs) & parameter($rhs)") {
          verify(evaluate(project(Literal(lhs), Parameter("rhs")), operatorIds = traceIds,
                          params = Map("rhs" -> rhs)))
        }

        test(s"$name: parameter($lhs) & literal($rhs)") {
          verify(evaluate(project(Parameter("lhs"), Literal(rhs)), operatorIds = traceIds,
                          params = Map("lhs" -> lhs)))
        }
    }
  }

  private def asAnyRef(value: Any) = value match {
    case x: Int => Int.box(x)
    case x: Long => Long.box(x)
    case x: Double => Double.box(x)
    case x: AnyRef => x
    case null => null
  }
}

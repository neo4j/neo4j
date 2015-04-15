/**
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
package org.neo4j.cypher.internal.compiler.v2_3.birk

import org.neo4j.cypher.internal.compiler.v2_3.CypherTypeException
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertions, Matchers, PropSpec}

class CompiledMathHelperTest extends PropSpec with TableDrivenPropertyChecks with Matchers with Assertions {

  val values: Seq[AnyRef] =
  // The exclamation mark casts the expression to a AnyRef instead of a primitive
  // To be as exhaustive as possible, the strategy is to do a cartesian product of all test values,
  // and ensure that either we have defined behaviour, or that a runtime type exception is thrown
    Seq(
      1 !,
      6789 !,
      null,
      "a",
      true !,
      false !
    )

  property("+") {
    forAll(getTable(CompiledMathHelper.add)) {
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (l: Number,            r: String,            Right(result)) => result should equal(l.toString + r)
      case (l: String,            r: Number,            Right(result)) => result should equal(l.toString + r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() + r.longValue())
      case (l: String,            r: String,            Right(result)) => result should equal(l + r)
      case (l: String,            r: java.lang.Boolean, Right(result)) => result should equal(l + r)
      case (l: java.lang.Boolean, r: String,            Right(result)) => result should equal(l + r)

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) => fail(s"Unspecified behaviour: $v1 + $v2 => $v3")
    }
  }

  implicit class A(i: Int) { def ! = i: java.lang.Integer }
  implicit class B(i: Boolean) { def ! = i: java.lang.Boolean }

  private def r(x: Any): AnyRef = x.asInstanceOf[AnyRef]

  private def getTable(f: (AnyRef, AnyRef) => AnyRef) = {

    val cartesianProduct = (for (x <- values; y <- values) yield {
      (x, y)
    }).distinct

    val valuesWithResults = cartesianProduct.map {
      case (l, r) => (l, r, throwableToLeft(f(l, r)))
    }

    Table(("lhs", "rhs", "result"), valuesWithResults: _*)
  }

  def throwableToLeft[T](block: => T): Either[java.lang.Throwable, T] =
    try {
      Right(block)
    } catch {
      case ex => Left(ex)
    }

}

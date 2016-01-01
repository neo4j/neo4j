/**
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
package org.neo4j.cypher.internal.compiler.v2_1.perty

import org.neo4j.cypher.internal.commons.CypherFunSuite
import scala.collection.mutable
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.simpleDocBuilder

class SimpleDocGeneratorTest extends CypherFunSuite {

  val docGen = simpleDocBuilder.docGenerator

  import Doc._

  test("DocGenerator renders primitive integers, longs, and doubles") {
    render(1) should equal("1")
    render(1L) should equal("1")
    render(1.0) should equal("1.0")
  }

  test("DocGenerator quotes strings") {
    render("") should equal("\"\"")
    render("a") should equal("\"a\"")
    render("\\") should equal("\"\\\\\"")
    render("'") should equal("\"\\'\"")
    render("\"") should equal("\"\\\"\"")
    render("\t") should equal("\"\\t\"")
    render("\b") should equal("\"\\b\"")
    render("\n") should equal("\"\\n\"")
    render("\r") should equal("\"\\r\"")
    render("\f") should equal("\"\\f\"")
  }

  test("DocGenerator quotes chars") {
    render('a') should equal("'a'")
    render('\'') should equal("'\\''")
    render('\"') should equal("'\\\"'")
    render('\t') should equal("'\\t'")
    render('\b') should equal("'\\b'")
    render('\n') should equal("'\\n'")
    render('\r') should equal("'\\r'")
    render('\f') should equal("'\\f'")
  }

  test("DocGenerator renders maps") {
    render(Map.empty) should equal("Map()")
    render(Map(1 -> "a")) should equal("Map(1 → \"a\")")
    render(Map(1 -> "a", 2 -> "b")) should equal("Map(1 → \"a\", 2 → \"b\")")
  }

  test("DocGenerator renders lists") {
    render(List.empty) should equal("⬨")
    render(List(1)) should equal("1 ⸬ ⬨")
    render(List(1, 2)) should equal("1 ⸬ 2 ⸬ ⬨")
  }

  test("DocGenerator renders immutable sets") {
    render(Set.empty) should equal("Set()")
    render(Set(1)) should equal("Set(1)")
    render(Set(1, 2)) should equal("Set(1, 2)")
  }

  test("DocGenerator renders mutable sets") {
    render(new mutable.HashSet) should equal("HashSet()")
    render((mutable.HashSet.newBuilder += 1 += 2).result()) should equal("HashSet(2, 1)")
  }

  test("DocGenerator renders non-list sequences") {
    render(Vector.empty) should equal("Vector()")
    render(Vector(1)) should equal("Vector(1)")
    render(Vector(1, 2)) should equal("Vector(1, 2)")
  }

  test("DocGenerator renders arrays") {
    render(Array.empty) should equal("Array()")
    render(Array(1)) should equal("Array(1)")
    render(Array(1, 2)) should equal("Array(1, 2)")
  }

  test("DocGenerator renders unit") {
    render(()) should equal("()")
  }

  test("DocGenerator catches ??? from inner doc generators") {
    object Fail {
      override def toString = throw new NotImplementedError
    }

    render(Fail) should equal("???")
  }

  test("DocGenerator renders products") {
    case object ZObj
    case class Y(v: Either[ZObj.type, Char])
    case class X[T](a: Y, b: T)

    render(X[Int]( a = Y(Left(ZObj)), b = 2 )) should equal("X(Y(Left(ZObj)), 2)")
    render(X[Int]( a = Y(Right('a')), b = 2 )) should equal("X(Y(Right('a')), 2)")
    render(X[(Int, Int)]( a = Y(Right('a')), b = (2, 3) )) should equal("X(Y(Right('a')), (2, 3))")
  }

  test("DocGenerator renders literal docs") {
    render(literal("a" :: "b")) should equal("DocLiteral(\"a\"·\"b\")")
  }

  test("DocGenerator uses toDoc as a fallback") {
    object Rick extends Pretty[Any] {
      def toDoc = "NEVER GONNA" :/: "GIVE YOU UP"
      override def toString = fail("Called toString when expected to be rendered using toDoc")
    }

    render(Rick) should equal("NEVER GONNA GIVE YOU UP")
  }

  private def render[T](v: T) =
    pformat(v, formatter = DocFormatters.defaultLineFormatter)(docGen)
}

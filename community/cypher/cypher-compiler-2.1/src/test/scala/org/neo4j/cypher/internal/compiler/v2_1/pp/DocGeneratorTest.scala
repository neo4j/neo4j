/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pp

import org.neo4j.cypher.internal.commons.CypherFunSuite
import scala.collection.mutable

class DocGeneratorTest extends CypherFunSuite {

  test("DocGenerator.forValues renders primitive integers, longs, and doubles") {
    implicit val docGen = DocGenerator.forValues

    render(1) should equal("1")
    render(1L) should equal("1")
    render(1.0) should equal("1.0")
  }

  test("DocGenerator.forValues quotes strings") {
    implicit val docGen = DocGenerator.forValues

    render("") should equal("\"\"")
    render("a") should equal("\"a\"")
    render("\\") should equal("\"\\\\\"")
    render("\"") should equal("\"\\\"\"")
    render("\t") should equal("\"\\t\"")
    render("\b") should equal("\"\\b\"")
    render("\n") should equal("\"\\n\"")
    render("\r") should equal("\"\\r\"")
  }

  test("DocGenerator.forValues quotes chars") {
    implicit val docGen = DocGenerator.forValues

    render('a') should equal("'a'")
    render('\'') should equal("'\\''")
    render('\t') should equal("'\\t'")
    render('\b') should equal("'\\b'")
    render('\n') should equal("'\\n'")
    render('\r') should equal("'\\r'")
  }

  test("DocGenerator.forValues renders maps") {
    implicit val docGen = DocGenerator.forValues

    render(Map.empty) should equal("Map()")
    render(Map(1 -> "a")) should equal("Map(1 → \"a\")")
    render(Map(1 -> "a", 2 -> "b")) should equal("Map(1 → \"a\", 2 → \"b\")")
  }

  test("DocGenerator.forValues renders lists") {
    implicit val docGen = DocGenerator.forValues

    render(List.empty) should equal("nil")
    render(List(1)) should equal("1 ⸬ nil")
    render(List(1, 2)) should equal("1 ⸬ 2 ⸬ nil")
  }

  test("DocGenerator.forValues renders immutable sets") {
    implicit val docGen = DocGenerator.forValues

    render(Set.empty) should equal("Set()")
    render(Set(1)) should equal("Set(1)")
    render(Set(1, 2)) should equal("Set(1, 2)")
  }

  test("DocGenerator.forValues renders mutable sets") {
    implicit val docGen = DocGenerator.forValues

    render(new mutable.HashSet) should equal("HashSet()")
    render((mutable.HashSet.newBuilder += 1 += 2).result()) should equal("HashSet(1, 2)")
  }

  test("DocGenerator.forValues renders non-list sequences") {
    implicit val docGen = DocGenerator.forValues

    render(Vector.empty) should equal("Vector()")
    render(Vector(1)) should equal("Vector(1)")
    render(Vector(1, 2)) should equal("Vector(1, 2)")
  }

  test("DocGenerator.forValues renders arrays") {
    implicit val docGen = DocGenerator.forValues

    render(Array.empty) should equal("Array()")
    render(Array(1)) should equal("Array(1)")
    render(Array(1, 2)) should equal("Array(1, 2)")
  }

  test("DocGenerator.forValues renders unit") {
    implicit val docGen = DocGenerator.forValues

    render(()) should equal("()")
  }

  test("DocGenerator.forValues catches ??? from inner doc generators") {
    implicit val docGen = DocGenerator.forValues

    object Fail {
      override def toString = ???
    }

    render(Fail) should equal("???")
  }

  test("DocGenerator.forValues renders products") {
    implicit val docGen = DocGenerator.forValues

    case object ZObj
    case class Y(v: Either[ZObj.type, Char])
    case class X[T](a: Y, b: T)

    render(X[Int]( a = Y(Left(ZObj)), b = 2 )) should equal("X(Y(Left(ZObj)), 2)")
    render(X[Int]( a = Y(Right('a')), b = 2 )) should equal("X(Y(Right('a')), 2)")
    render(X[(Int, Int)]( a = Y(Right('a')), b = (2, 3) )) should equal("X(Y(Right('a')), (2, 3))")

  }

  private def render[T](v: T)(implicit docGen: DocGenerator[T]) =
    pp.format(v, formatter = DocFormatter.defaultLineFormatter)(docGen)
}

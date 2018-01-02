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
package org.neo4j.cypher.internal.frontend.v2_3.perty.gen

import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.SimpleDocHandler
import org.neo4j.cypher.internal.frontend.v2_3.perty.{ConsDoc, DocLiteral, NilDoc, TextDoc}

import scala.collection.mutable

class SimpleDocGenTest extends DocHandlerTestSuite[Any] {

  val docGen = SimpleDocHandler.docGen

  test("simpleDocGen formats primitive integers, longs, and doubles") {
    pprintToString(1) should equal("1")
    pprintToString(1L) should equal("1")
    pprintToString(1.0) should equal("1.0")
  }

  test("simpleDocGen quotes strings") {
    pprintToString("") should equal("\"\"")
    pprintToString("a") should equal("\"a\"")
    pprintToString("\\") should equal("\"\\\\\"")
    pprintToString("'") should equal("\"\\'\"")
    pprintToString("\"") should equal("\"\\\"\"")
    pprintToString("\t") should equal("\"\\t\"")
    pprintToString("\b") should equal("\"\\b\"")
    pprintToString("\n") should equal("\"\\n\"")
    pprintToString("\r") should equal("\"\\r\"")
    pprintToString("\f") should equal("\"\\f\"")
  }

  test("simpleDocGen quotes chars") {
    pprintToString('a') should equal("'a'")
    pprintToString('\'') should equal("'\\''")
    pprintToString('\"') should equal("'\\\"'")
    pprintToString('\t') should equal("'\\t'")
    pprintToString('\b') should equal("'\\b'")
    pprintToString('\n') should equal("'\\n'")
    pprintToString('\r') should equal("'\\r'")
    pprintToString('\f') should equal("'\\f'")
  }

  test("simpleDocGen formats immutable maps") {
    pprintToString(Map.empty) should equal("Map()")
    pprintToString(Map(1 -> "a")) should equal("Map(1 → \"a\")")
    pprintToString(Map(1 -> "a", 2 -> "b")) should equal("Map(1 → \"a\", 2 → \"b\")")
  }

  test("simpleDocGen formats mutable maps") {
    pprintToString(mutable.OpenHashMap.empty) should equal("OpenHashMap()")
    pprintToString(mutable.OpenHashMap(1 -> "a")) should equal("OpenHashMap(1 → \"a\")")
    pprintToString(mutable.OpenHashMap(1 -> "a", 2 -> "b")) should equal("OpenHashMap(1 → \"a\", 2 → \"b\")")
  }

  test("simpleDocGen formats lists") {
    pprintToString(List.empty) should equal("⬨")
    pprintToString(List(1)) should equal("1 ⸬ ⬨")
    pprintToString(List(1, 2)) should equal("1 ⸬ 2 ⸬ ⬨")
  }

  test("simpleDocGen formats immutable sets") {
    pprintToString(Set.empty) should equal("Set()")
    pprintToString(Set(1)) should equal("Set(1)")
    pprintToString(Set(1, 2)) should equal("Set(1, 2)")
  }

  test("simpleDocGen formats mutable sets") {
    val emptySet = pprintToString(new mutable.HashSet)
    emptySet should equal("HashSet()")

    val filledSet = pprintToString((mutable.HashSet.newBuilder += 1 += 2).result())
    (filledSet == "HashSet(2, 1)" || filledSet == "HashSet(1, 2)") should be(true)
  }

  test("simpleDocGen formats non-list sequences") {
    pprintToString(Vector.empty) should equal("Vector()")
    pprintToString(Vector(1)) should equal("Vector(1)")
    pprintToString(Vector(1, 2)) should equal("Vector(1, 2)")
  }

  test("simpleDocGen formats arrays") {
    pprintToString(Array.empty) should equal("Array()")
    pprintToString(Array(1)) should equal("Array(1)")
    pprintToString(Array("x")) should equal("Array(\"x\")")
    pprintToString(Array(1, 2)) should equal("Array(1, 2)")
  }

  test("simpleDocGen formats unit") {
    pprintToString(()) should equal("()")
  }

  test("simpleDocGen catches ??? from inner doc generators") {
    object Fail {
      override def toString = throw new NotImplementedError
    }

    pprintToString(Fail) should equal("???")
  }

  test("simpleDocGen formats products") {
    import SimpleDocGenTest._

    pprintToString(ZObj) should equal("ZObj")
    pprintToString(Right('a')) should equal("Right('a')")
    pprintToString(Y(Left(ZObj))) should equal("Y(Left(ZObj))")
    pprintToString(X[Int]( a = Y(Left(ZObj)), b = 2 )) should equal("X(Y(Left(ZObj)), 2)")
    pprintToString(X[Int]( a = Y(Right('a')), b = 2 )) should equal("X(Y(Right('a')), 2)")
    pprintToString(X[(Int, Int)]( a = Y(Right('a')), b = (2, 3) )) should equal("X(Y(Right('a')), (2, 3))")
  }

  test("simpleDocGen formats literal docs") {
    val result = pprintToString(DocLiteral(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc))))

    result should equal("DocLiteral(\"a\" ⸬ \"b\" ⸬ ø)")
  }
}

object SimpleDocGenTest {
  case object ZObj
  case class Y(v: Either[ZObj.type, Char])
  case class X[T](a: Y, b: T)
}

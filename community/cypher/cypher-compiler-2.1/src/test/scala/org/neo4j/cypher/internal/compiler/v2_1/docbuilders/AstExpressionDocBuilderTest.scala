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
package org.neo4j.cypher.internal.compiler.v2_1.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.{simpleDocBuilder, DocBuilderTestSuite}

class AstExpressionDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder = astExpressionDocBuilder orElse simpleDocBuilder

  test("Identifier(\"a\") => a") {
    format(ident("a")) should equal("a")
  }

  test("Property(map, name) => map.name") {
    val expr: Expression = Property(ident("a"), PropertyKeyName("name")_)_
    format(expr) should equal("a.name")
  }

  test("HasLabel(n, Seq(LabelName(\"Label\"))) => n:Label") {
    val expr: Expression = HasLabels(ident("a"), Seq(LabelName("Person")_))_
    format(expr) should equal("a:Person")
  }

  test("HasLabel(n, Seq(LabelName(\"Label1\"), LabelName(\"Label2\"))) => n:Label1:Label2") {
    val expr: Expression = HasLabels(ident("a"), Seq(LabelName("Person")_, LabelName("PartyAnimal")_))_
    format(expr) should equal("a:Person:PartyAnimal")
  }

  test("Not(left) => NOT left") {
    val expr: Expression = Not(ident("a"))_
    format(expr) should equal("NOT a")
  }

  test("And(left, right) => left AND right") {
    val expr: Expression = And(ident("a"), ident("b"))_
    format(expr) should equal("a AND b")
  }

  test("Or(left, right) => left OR right") {
    val expr: Expression = Or(ident("a"), ident("b"))_
    format(expr) should equal("a OR b")
  }

  test("Xor(left, right) => left XOR right") {
    val expr: Expression = Xor(ident("a"), ident("b"))_
    format(expr) should equal("a XOR b")
  }

  test("Equals(left, right) => left = right") {
    val expr: Expression = Equals(ident("a"), ident("b"))_
    format(expr) should equal("a = b")
  }

  test("NotEquals(left, right) => left <> right") {
    val expr: Expression = NotEquals(ident("a"), ident("b"))_
    format(expr) should equal("a <> b")
  }

  test("LessThan(left, right) => left < right") {
    val expr: Expression = LessThan(ident("a"), ident("b"))_
    format(expr) should equal("a < b")
  }

  test("LessThanOrEqual(left, right) => left <= right") {
    val expr: Expression = LessThanOrEqual(ident("a"), ident("b"))_
    format(expr) should equal("a <= b")
  }

  test("GreaterThan(left, right) => left > right") {
    val expr: Expression = GreaterThan(ident("a"), ident("b"))_
    format(expr) should equal("a > b")
  }

  test("GreaterThanOrEqual(left, right) => left >= right") {
    val expr: Expression = GreaterThanOrEqual(ident("a"), ident("b"))_
    format(expr) should equal("a >= b")
  }

  test("Number literals are printed as string value") {
    val expr: Expression = SignedDecimalIntegerLiteral("1")_
    format(expr) should equal("1")
  }

  test("True()") {
    val expr: Expression = True()_
    format(expr) should equal("true")
  }

  test("False()") {
    val expr: Expression = False()_
    format(expr) should equal("false")
  }

  test("Null()") {
    val expr: Expression = Null()_
    format(expr) should equal("NULL")
  }

  test("string literals are printed quoted") {
    val expr: Expression = StringLiteral("a")_
    format(expr) should equal("\"a\"")
  }

  test("FunctionInvocation(FunctionName(\"split\"), distinct = false, Vector(fst, snd)) => split(fst, snd)") {
    val name: FunctionName = FunctionName("split")(pos)
    val args: IndexedSeq[Expression] = Vector(SignedDecimalIntegerLiteral("1")_, SignedDecimalIntegerLiteral("2")_)
    val expr: Expression = FunctionInvocation( functionName = name, distinct = false, args = args)_

    format(expr) should equal("split(1, 2)")
  }

  test("FunctionInvocation(FunctionName(\"split\"), distinct = true, Vector(fst, snd)) => split(fst, snd)") {
    val name: FunctionName = FunctionName("split")(pos)
    val args: IndexedSeq[Expression] = Vector(SignedDecimalIntegerLiteral("1")_, SignedDecimalIntegerLiteral("2")_)
    val expr: Expression = FunctionInvocation( functionName = name, distinct = true, args = args)_

    format(expr) should equal("DISTINCT split(1, 2)")
  }
}

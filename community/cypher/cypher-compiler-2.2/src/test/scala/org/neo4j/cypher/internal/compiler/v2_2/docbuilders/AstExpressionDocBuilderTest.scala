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
package org.neo4j.cypher.internal.compiler.v2_2.docbuilders

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.{DocBuilderTestSuite, simpleDocBuilder}

class AstExpressionDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder = astExpressionDocBuilder orElse astParticleDocBuilder orElse simpleDocBuilder

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

  test("IsNull(left) => left IS NULL") {
    val expr: Expression = IsNull(ident("a"))_
    format(expr) should equal("a IS NULL")
  }

  test("IsNotNull(left) => left IS NOT NULL") {
    val expr: Expression = IsNotNull(ident("a"))_
    format(expr) should equal("a IS NOT NULL")
  }

  test("And(left, right) => left AND right") {
    val expr: Expression = And(ident("a"), ident("b"))_
    format(expr) should equal("a AND b")
  }

  test("Ands(a, b, c) => a AND b AND c") {
    val expr: Expression = Ands(Set(ident("a"), ident("b"), ident("c")))_
    format(expr) should equal("a AND b AND c")
  }

  test("Ands(a) => a") {
    val expr: Expression = Ands(Set(ident("a")))_
    format(expr) should equal("a")
  }

  test("Or(left, right) => left OR right") {
    val expr: Expression = Or(ident("a"), ident("b"))_
    format(expr) should equal("a OR b")
  }

  test("Or(a, b, c) => a OR b OR c") {
    val expr: Expression = Ors(Set(ident("a"), ident("b"), ident("c")))_
    format(expr) should equal("a OR b OR c")
  }

  test("Ors(a) => a") {
    val expr: Expression = Ors(Set(ident("a")))_
    format(expr) should equal("a")
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

  test("InvalidNotEquals(left, right) => left <> right") {
    val expr: Expression = InvalidNotEquals(ident("a"), ident("b"))_
    format(expr) should equal("a != b")
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

  test("In(left, right) => left IN right") {
    val expr: Expression = In(ident("a"), ident("b"))_
    format(expr) should equal("a IN b")
  }

  test("RegexMatch(left, right) => left =~ right") {
    val expr: Expression = RegexMatch(ident("a"), ident("b"))_
    format(expr) should equal("a =~ b")
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

  test("Collection(a, b) => [a, b]") {
    val expr: Collection = Collection(Seq(ident("a"), ident("b")))_

    format(expr) should equal("[a, b]")
  }

  test("Collection(a) => [a]") {
    val expr: Collection = Collection(Seq(ident("a")))_

    format(expr) should equal("[a]")
  }

  test("Collection() => []") {
    val expr: Collection = Collection(Seq())_

    format(expr) should equal("[]")
  }

  test("Parameter(x) => {x}") {
    format(Parameter("x")(pos)) should equal("{x}")
  }

  test("count(*)") {
    format(CountStar()(pos)) should equal("count(*)")
  }
}

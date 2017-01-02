/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.predicates

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import collection.mutable

class CheckerTest extends CypherFunSuite {

  test("iterator gets emptied checking for a value") {
    val buildUp = new BuildUp(Iterator(42, 123))
    val (result, newChecker) = buildUp.contains("hello")
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]
    newChecker.contains("hello") should equal((Some(false), newChecker))
  }

  test("the same value is checked twice") {
    val input = Iterator[Any](42, "123")
    val buildUp = new BuildUp(input)
    buildUp.contains(42) should equal((Some(true), buildUp))
    buildUp.contains(42) should equal((Some(true), buildUp))
    input should not be 'empty
    val (result, newChecker) = buildUp.contains("123")
    input shouldBe 'empty
    newChecker shouldBe a [SetChecker]
    result should equal(Some(true))
  }

  test("checking for lists in lists") {
    val input = Iterator(Array(1, 2, 3), List(1, 2))
    val buildUp = new BuildUp(input)
    buildUp.contains(List(1, 2, 3)) should equal((Some(true), buildUp))
  }

  test("type tests") {
    (equi(1) == equi(1.0)) should equal(true)
    (equi('a') == equi("a")) should equal(true)
    (eq(Array(1, 2, 3)) == eq(List(1.0, 2.0D, 3l))) should equal(true)
  }

  test("null in lhs on an BuildUp") {
    val buildUp = new BuildUp(Iterator(42))
    buildUp.contains(null) should equal((None, buildUp))
  }

  test("null in lhs on an FastChecker") {
    val fastChecker = new SetChecker(mutable.Set(equi(42)), Some(false))
    fastChecker.contains(null) should equal((None, fastChecker))
  }

  test("null in rhs on an FastChecker") {
    val fastChecker = new SetChecker(mutable.Set(equi(42)), None)
    fastChecker.contains(4) should equal((None, fastChecker))
    fastChecker.contains(42) should equal((Some(true), fastChecker))
    fastChecker.contains(null) should equal((None, fastChecker))
  }

  test("null in the list") {
    val input = Iterator[Any](42, null)
    val buildUp = new BuildUp(input)
    buildUp.contains(42) should equal((Some(true), buildUp))
    val (result, newChecker) = buildUp.contains("hullo")
    result should equal(None)
    newChecker shouldBe a[SetChecker]
  }

  test("buildUp can handle maps on the lhs") {
    val buildUp = new BuildUp(Iterator(1,2,3))
    val (result, newChecker) = buildUp.contains(Map("a" -> 42))
    result should equal(Some(false))
    newChecker shouldBe a [SetChecker]
  }

  test("fastChecker can handle maps on the lhs") {
    val buildUp = new SetChecker(mutable.Set(equi(1),equi(2),equi(3)), Some(false))
    val (result, newChecker) = buildUp.contains(Map("a" -> 42))
    result should equal(Some(false))
    newChecker shouldBe a [SetChecker]
  }

  test("buildUp can handle maps on the rhs") {
    val buildUp = new BuildUp(Iterator(1,2,3, Map("a" -> 42)))
    val (result, newChecker) = buildUp.contains("apa")
    result should equal(Some(false))
    newChecker shouldBe a [SetChecker]
  }

  test("buildUp handles a single null in the collection") {
    val buildUp = new BuildUp(Iterator(null))
    val (result, newChecker) = buildUp.contains("apa")
    result should equal(None)
    newChecker shouldBe a [NullListChecker.type]
  }

  private def equi(x: Any): Equivalent = Equivalent(x)
}

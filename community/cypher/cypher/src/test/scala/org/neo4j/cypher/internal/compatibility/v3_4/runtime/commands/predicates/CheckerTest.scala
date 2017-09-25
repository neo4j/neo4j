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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.ValueConversion
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{NO_VALUE, intValue, stringValue}
import org.neo4j.values.virtual.VirtualValues.{list, map}

import scala.collection.mutable

class CheckerTest extends CypherFunSuite {

  test("iterator gets emptied checking for a value") {
    val buildUp = new BuildUp(iterator(42, 123))
    val (result, newChecker) = buildUp.contains(stringValue("hello"))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]
    newChecker.contains(stringValue("hello")) should equal((Some(false), newChecker))
  }

  test("the same value is checked twice") {
    val buildUp = new BuildUp(iterator(42, "123"))
    buildUp.contains(intValue(42)) should equal((Some(true), buildUp))
    buildUp.contains(intValue(42)) should equal((Some(true), buildUp))
    buildUp.iterator.hasNext shouldBe true
    val (result, newChecker) = buildUp.contains(stringValue("123"))
    buildUp.iterator.hasNext shouldBe false
    newChecker shouldBe a[SetChecker]
    result should equal(Some(true))
  }

  test("checking for lists in lists") {
    val input = iterator(Array(1, 2, 3), List(1, 2))
    val buildUp = new BuildUp(input)
    buildUp.contains(list(intValue(1), intValue(2), intValue(3))) should equal((Some(true), buildUp))
  }

  test("null in lhs on an BuildUp") {
    val buildUp = new BuildUp(iterator(42))
    buildUp.contains(NO_VALUE) should equal((None, buildUp))
  }

  test("null in lhs on an FastChecker") {
    val fastChecker = new SetChecker(mutable.Set(intValue(42)), Some(false))
    fastChecker.contains(Values.NO_VALUE) should equal((None, fastChecker))
  }

  test("null in rhs on an FastChecker") {
    val fastChecker = new SetChecker(mutable.Set(intValue(42)), None)
    fastChecker.contains(intValue(4)) should equal((None, fastChecker))
    fastChecker.contains(intValue(42)) should equal((Some(true), fastChecker))
    fastChecker.contains(Values.NO_VALUE) should equal((None, fastChecker))
  }

  test("null in the list") {
    val input = iterator(42, null)
    val buildUp = new BuildUp(input)
    buildUp.contains(intValue(42)) should equal((Some(true), buildUp))
    val (result, newChecker) = buildUp.contains(stringValue("hullo"))
    result should equal(None)
    newChecker shouldBe a[SetChecker]
  }

  test("buildUp can handle maps on the lhs") {
    val buildUp = new BuildUp(iterator(1, 2, 3))
    val (result, newChecker) = buildUp.contains(map(Array("a"), Array[AnyValue](intValue(42))))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]
  }

  test("fastChecker can handle maps on the lhs") {
    val buildUp = new SetChecker(mutable.Set(intValue(1), intValue(2), intValue(3)), Some(false))
    val (result, newChecker) = buildUp.contains(map(Array("a"), Array[AnyValue](intValue(42))))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]
  }

  test("buildUp can handle maps on the rhs") {
    val buildUp = new BuildUp(iterator(1, 2, 3, Map("a" -> 42)))
    val (result, newChecker) = buildUp.contains(stringValue("apa"))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]
  }

  test("buildUp handles a single null in the collection") {
    val buildUp = new BuildUp(iterator(null))
    val (result, newChecker) = buildUp.contains(stringValue("apa"))
    result should equal(None)
    newChecker shouldBe a[NullListChecker.type]
  }

  private def iterator(a: Any*) = list(a.map(ValueConversion.asValue):_*)

}

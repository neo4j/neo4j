/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.interpreted.ValueConversion
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.values.virtual.VirtualValues.map

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
    newChecker.contains(list(NO_VALUE))._1 should equal(None)
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

  test("buildUp handles null and non nulls") {
    val buildUp = new BuildUp(iterator(1, null, 3, 4))
    val (result, newChecker) = buildUp.contains(intValue(3))
    result should equal(Some(true))
    newChecker shouldBe a[BuildUp]

    val (result2, newChecker2) = newChecker.contains(intValue(4))
    result2 should equal(Some(true))
    newChecker2 shouldBe a[SetChecker]

    val (result3, _) = newChecker2.contains(intValue(5))
    result3 should equal(None)
  }

  test("handles arrays with null on buildup") {
    val buildUp = new BuildUp(iterator(Array(1), Array(2)))
    val (result, newChecker) = buildUp.contains(iterator(null))
    result should equal(None)
    newChecker shouldBe a[SetChecker]

    val (result2, _) = newChecker.contains(iterator(0))
    result2 should equal(Some(false))

    val (result3, _) = newChecker.contains(stringValue("apa"))
    result3 should equal(Some(false))
  }

  test("handles arrays with even more null on buildup") {
    val buildUp = new BuildUp(iterator(Array(1), Array(2), "oh no"))
    val (result, newChecker) = buildUp.contains(iterator(2))
    result should equal(Some(true))
    newChecker shouldBe a[BuildUp]

    val (result2, _) = newChecker.contains(iterator(null))
    result2 should equal(None)
  }

  test("handles maps with null on buildup") {
    val buildUp = new BuildUp(iterator(Map("a" -> 1)))
    val (result, newChecker) = buildUp.contains(VirtualValues.map(Array("a"), Array(NO_VALUE)))
    result should equal(None)
    newChecker shouldBe a[SetChecker]

    val (result2, newChecker2) = newChecker.contains(VirtualValues.map(Array("a"), Array(intValue(0))))
    result2 should equal(Some(false))

    val (result3, _) = newChecker2.contains(stringValue("apa"))
    result3 should equal(Some(false))
  }

  test("handles maps with even more null on buildup") {
    val buildUp = new BuildUp(iterator(Map("a" -> 1), 1, "oh no"))
    val (result, newChecker) = buildUp.contains(intValue(1))
    result should equal(Some(true))
    newChecker shouldBe a[BuildUp]

    val (result2, newChecker2) = newChecker.contains(VirtualValues.map(Array("a"), Array(NO_VALUE)))
    result2 should equal(None)
  }

  test("handles arrays with null after buildup") {
    val buildUp = new BuildUp(iterator(Array(1), Array(2)))
    val (result, newChecker) = buildUp.contains(stringValue("apa"))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]

    val (result2, newChecker2) = newChecker.contains(iterator(0))
    result2 should equal(Some(false))

    val (result3, newChecker3) = newChecker2.contains(iterator(null))
    result3 should equal(None)

    val (result4, _) = newChecker3.contains(iterator(2))
    result4 should equal(Some(true))
  }

  test("handles maps with null after buildup") {
    val buildUp = new BuildUp(iterator(Map("a" -> 1)))
    val (result, newChecker) = buildUp.contains(stringValue("apa"))
    result should equal(Some(false))
    newChecker shouldBe a[SetChecker]

    val (result2, newChecker2) = newChecker.contains(VirtualValues.map(Array("a"), Array(intValue(0))))
    result2 should equal(Some(false))

    val (result3, newChecker3) = newChecker2.contains(VirtualValues.map(Array("a"), Array(NO_VALUE)))
    result3 should equal(None)

    val (result4, _) = newChecker3.contains(VirtualValues.map(Array("a"), Array(intValue(1))))
    result4 should equal(Some(true))
  }

  private def iterator(a: Any*) = list(a.map(ValueConversion.asValue):_*)
}

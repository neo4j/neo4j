/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.evaluator

import java.lang.Math.{PI, sin}

import org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{intValue, pointValue, stringValue}
import org.neo4j.values.virtual.VirtualValues.{list, map}
import org.scalatest._

class SimpleInternalExpressionEvaluatorTest extends FunSuiteLike with Matchers {

  test("parse literals") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator.evaluate("'hello'") should equal(stringValue("hello"))
    evaluator.evaluate("42") should equal(intValue(42))
    evaluator.evaluate("false") should equal(Values.FALSE)
    evaluator.evaluate("[1,'foo', true]") should equal(list(intValue(1), stringValue("foo"), Values.TRUE))
    evaluator.evaluate("{prop1: 42}") should equal(map(Array("prop1"), Array(intValue(42))))
  }

  test("list comprehensions") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator.evaluate("[x IN range(0,10) WHERE x % 2 = 0 | x^3]") should equal(
      list(intValue(0), intValue(8), intValue(64), intValue(216), intValue(512), intValue(1000)))
  }

  test("functions") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator.evaluate("point({ latitude: 12, longitude: 56, height: 1000 })") should
      equal(pointValue(WGS84_3D, 56, 12, 1000))
    evaluator.evaluate("sin(pi())") should equal(Values.doubleValue(sin(PI)))
  }
}

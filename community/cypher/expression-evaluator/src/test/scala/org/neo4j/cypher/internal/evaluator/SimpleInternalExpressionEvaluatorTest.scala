/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.evaluator

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.pointValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.values.virtual.VirtualValues.map
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.lang.Math.PI
import java.lang.Math.sin

class SimpleInternalExpressionEvaluatorTest extends AnyFunSuiteLike with Matchers {

  val evaluator = new SimpleInternalExpressionEvaluator

  test("parse literals") {
    evaluate("'hello'") should equal(stringValue("hello"))
    evaluate("42") should equal(intValue(42))
    evaluate("false") should equal(Values.FALSE)
    evaluate("[1,'foo', true]") should equal(list(intValue(1), stringValue("foo"), Values.TRUE))
    evaluate("{prop1: 42}") should equal(map(Array("prop1"), Array(intValue(42))))
  }

  test("list comprehensions") {
    evaluate("[x IN range(0,10) WHERE x % 2 = 0 | x^3]") should equal(
      list(intValue(0), intValue(8), intValue(64), intValue(216), intValue(512), intValue(1000))
    )
  }

  test("functions") {
    evaluate("point({ latitude: 12, longitude: 56, height: 1000 })") should
      equal(pointValue(WGS_84_3D, 56, 12, 1000))
    evaluate("sin(pi())") should equal(Values.doubleValue(sin(PI)))
  }

  test("params") {
    evaluator
      .evaluate(
        expression = parse("$p + 1"),
        params = VirtualValues.map(Array("p"), Array(Values.of(2)))
      )
      .shouldEqual(Values.of(3))
  }

  test("params in functions") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator
      .evaluate(
        expression = parse("sin(pi()*$p/180) + cos(pi()*$q/180)"),
        params = VirtualValues.map(Array("p", "q"), Array(Values.of(90), Values.of(0)))
      )
      .shouldEqual(Values.of(2))
  }

  test("context") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator
      .evaluate(
        expression = parse("v + 1"),
        context = CypherRow.from("v" -> Values.of(3))
      )
      .shouldEqual(Values.of(4))
  }

  test("context in functions") {
    val evaluator = new SimpleInternalExpressionEvaluator

    evaluator
      .evaluate(
        expression = parse(
          "point({ latitude: p.y, longitude: p.x, height: 1000 })"
        ),
        context = CypherRow.from("p" -> Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 56, 12))
      )
      .shouldEqual(pointValue(WGS_84_3D, 56, 12, 1000))
  }

  private def parse(cypher: String): Expression =
    AstParserFactory
      .apply(CypherVersion.Default)
      .apply(cypher, Neo4jCypherExceptionFactory(cypher, None), None)
      .expression()

  private def evaluate(cypher: String): AnyValue = evaluator.evaluate(parse(cypher))
}

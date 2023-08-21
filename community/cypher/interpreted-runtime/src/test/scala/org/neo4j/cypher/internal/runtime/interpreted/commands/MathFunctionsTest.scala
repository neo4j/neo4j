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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbsFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AcosFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AsinFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Atan2Function
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AtanFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CeilFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CosFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CotFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.DegreesFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.EFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.FloorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.IsNaNFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Log10Function
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.LogFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper.asDouble
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PiFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Pow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.RadiansFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.RoundFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SignFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SinFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SqrtFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.TanFunction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.longValue

class MathFunctionsTest extends CypherFunSuite {

  test("absTests") {
    calc(AbsFunction(literal(-1))) should equal(longValue(1))
    calc(AbsFunction(literal(1))) should equal(longValue(1))
    intercept[CypherTypeException](calc(AbsFunction(literal("wut"))))
  }

  test("abs should give only longs back on integral input") {
    calc(AbsFunction(literal(Byte.box(-1)))) should equal(longValue(1L))
    calc(AbsFunction(literal(Short.box(-1)))) should equal(longValue(1L))
    calc(AbsFunction(literal(Int.box(-1)))) should equal(longValue(1L))
    calc(AbsFunction(literal(Long.box(-1)))) should equal(longValue(1L))
  }

  test("abs should give only doubles back on integral input") {
    calc(AbsFunction(literal(Float.box(-1.5f)))) should equal(doubleValue(1.5))
    calc(AbsFunction(literal(Double.box(-1.5)))) should equal(doubleValue(1.5))
  }

  test("acosTests") {
    asDouble(calc(AcosFunction(literal(.7)))).doubleValue() should equal(0.795398830184144 +- 0.00001)
    intercept[CypherTypeException](calc(AcosFunction(literal("wut"))))
  }

  test("asinTests") {
    asDouble(calc(AsinFunction(literal(.7)))).doubleValue() should equal(0.775397496610753 +- 0.00001)
    intercept[CypherTypeException](calc(AsinFunction(literal("wut"))))
  }

  test("atanTests") {
    asDouble(calc(AtanFunction(literal(.7)))).doubleValue() should equal(0.610725964389209 +- 0.00001)
    intercept[CypherTypeException](calc(AtanFunction(literal("wut"))))
  }

  test("atan2Tests") {
    asDouble(calc(Atan2Function(literal(.7), literal(.8)))).doubleValue() should equal(0.718829999621624 +- 0.00001)
    asDouble(calc(Atan2Function(literal(.8), literal(.8)))).doubleValue() should equal(0.785398163397448 +- 0.00001)
    intercept[CypherTypeException](calc(Atan2Function(literal("wut"), literal(.7))))
  }

  test("ceilTests") {
    asDouble(calc(CeilFunction(literal(.7)))).doubleValue() should equal(1.0 +- 0.00001)
    asDouble(calc(CeilFunction(literal(-.7)))).doubleValue() should equal(0.0 +- 0.00001)
    intercept[CypherTypeException](calc(CeilFunction(literal("wut"))))
  }

  test("cosTests") {
    asDouble(calc(CosFunction(literal(.7)))).doubleValue() should equal(0.764842187284489 +- 0.00001)
    intercept[CypherTypeException](calc(CosFunction(literal("wut"))))
  }

  test("cotTests") {
    asDouble(calc(CotFunction(literal(.7)))).doubleValue() should equal(1.18724183212668 +- 0.00001)
    asDouble(calc(CotFunction(literal(0.0)))).doubleValue() should equal(Double.PositiveInfinity +- 0.00001)
    intercept[CypherTypeException](calc(CotFunction(literal("wut"))))
  }

  test("degreesTests") {
    asDouble(calc(DegreesFunction(literal(0.785398163397448)))).doubleValue() should equal(45.0 +- 0.00001)
    asDouble(calc(DegreesFunction(literal(0.0)))).doubleValue() should equal(0.0 +- 0.00001)
    intercept[CypherTypeException](calc(DegreesFunction(literal("wut"))))
  }

  test("eTests") {
    asDouble(calc(EFunction())).doubleValue() should equal(2.718281828459045 +- 0.00001)
  }

  test("expTests") {
    asDouble(calc(ExpFunction(literal(3.2)))).doubleValue() should equal(24.532530197109352 +- 0.00001)
    intercept[CypherTypeException](calc(ExpFunction(literal("wut"))))
  }

  test("floorTests") {
    asDouble(calc(FloorFunction(literal(0.9)))).doubleValue() should equal(0.0 +- 0.00001)
    asDouble(calc(FloorFunction(literal(-0.9)))).doubleValue() should equal(-1.0 +- 0.00001)
    intercept[CypherTypeException](calc(FloorFunction(literal("wut"))))
  }

  test("isNaNTests") {
    calc(IsNaNFunction(literal(1.0f))).asInstanceOf[BooleanValue].booleanValue() should equal(false)
    calc(IsNaNFunction(literal(Double.PositiveInfinity))).asInstanceOf[BooleanValue].booleanValue() should equal(false)
    calc(IsNaNFunction(literal(Double.NaN))).asInstanceOf[BooleanValue].booleanValue() should equal(true)

    the[CypherTypeException] thrownBy {
      calc(IsNaNFunction(literal("foo")))
    } should have message "isNaN() requires numbers"
  }

  test("logTests") {
    asDouble(calc(LogFunction(literal(27.0)))).doubleValue() should equal(3.295836866004329 +- 0.00001)
    intercept[CypherTypeException](calc(LogFunction(literal("wut"))))
  }

  test("log10Tests") {
    asDouble(calc(Log10Function(literal(27.0)))).doubleValue() should equal(1.4313637641589874 +- 0.00001)
    intercept[CypherTypeException](calc(Log10Function(literal("wut"))))
  }

  test("piTests") {
    asDouble(calc(PiFunction())).doubleValue() should equal(3.141592653589793 +- 0.00001)
  }

  test("radiansTests") {
    asDouble(calc(RadiansFunction(literal(45.0)))).doubleValue() should equal(0.785398163397448 +- 0.00001)
    intercept[CypherTypeException](calc(RadiansFunction(literal("wut"))))
  }

  test("signTests") {
    calc(SignFunction(literal(-1))).asInstanceOf[LongValue].longValue() should equal(-1L)
    calc(SignFunction(literal(1))).asInstanceOf[LongValue].longValue() should equal(1L)
    calc(SignFunction(literal(Double.NegativeInfinity))).asInstanceOf[LongValue].longValue() should equal(-1L)
    calc(SignFunction(literal(Math.PI))).asInstanceOf[LongValue].longValue() should equal(1L)
    intercept[CypherTypeException](calc(SignFunction(literal("wut"))))
  }

  test("sinTests") {
    asDouble(calc(SinFunction(literal(0.7)))).doubleValue() should equal(0.644217687237691 +- 0.00001)
    intercept[CypherTypeException](calc(SinFunction(literal("wut"))))
  }

  test("tanTests") {
    asDouble(calc(TanFunction(literal(0.7)))).doubleValue() should equal(0.8422883804630794 +- 0.00001)
    intercept[CypherTypeException](calc(TanFunction(literal("wut"))))
  }

  test("roundTests") {
    calc(RoundFunction(literal(1.5), literal(0), literal("HALF_UP"), literal(true))) should equal(doubleValue(2))
    calc(RoundFunction(literal(1.5), literal(0), literal("HALF_UP"), literal(false))) should equal(doubleValue(2))
    calc(RoundFunction(literal(1.5), literal(0), literal("HALF_DOWN"), literal(true))) should equal(doubleValue(1))
    calc(RoundFunction(literal(-1.5), literal(0), literal("HALF_UP"), literal(true))) should equal(doubleValue(-2))
    calc(RoundFunction(literal(-1.5), literal(0), literal("HALF_UP"), literal(false))) should equal(doubleValue(-1))
    calc(RoundFunction(literal(-1.5), literal(0), literal("HALF_DOWN"), literal(true))) should equal(doubleValue(-1))
    calc(RoundFunction(literal(12.22), literal(0), literal("HALF_UP"), literal(false))) should equal(doubleValue(12))
    calc(RoundFunction(literal(12.22), literal(1), literal("HALF_UP"), literal(false))) should equal(doubleValue(12.2))
    intercept[CypherTypeException](calc(RoundFunction(literal("wut"), literal(0), literal("HALF_UP"), literal(false))))
    intercept[CypherTypeException](calc(RoundFunction(
      literal(1.5),
      literal("wut"),
      literal("HALF_UP"),
      literal(false)
    )))
    intercept[CypherTypeException](calc(RoundFunction(literal(1.5), literal(1), literal(42), literal(true))))
  }

  test("powFunction") {
    calc(Pow(literal(2), literal(4))) should equal(doubleValue(math.pow(2, 4)))
    intercept[CypherTypeException](calc(Pow(literal("wut"), literal(2))))
    intercept[CypherTypeException](calc(Pow(literal(3.1415), literal("baaaah"))))
  }

  test("sqrtFunction") {
    calc(SqrtFunction(literal(16))) should equal(doubleValue(4))
    intercept[CypherTypeException](calc(SqrtFunction(literal("wut"))))
  }

  private def calc(e: Expression) = e(CypherRow.empty, QueryStateHelper.empty)
}

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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class MathFunctionsTest extends CypherFunSuite with NumericHelper {

  test("absTests") {
    calc(AbsFunction(Literal(-1))) should equal(1)
    calc(AbsFunction(Literal(1))) should equal(1)
    intercept[CypherTypeException](calc(AbsFunction(Literal("wut"))))
  }

  test("acosTests") {
    asDouble(calc(AcosFunction(Literal(.7)))) should equal(0.795398830184144 +- 0.00001)
    intercept[CypherTypeException](calc(AcosFunction(Literal("wut"))))
  }

  test("asinTests") {
    asDouble(calc(AsinFunction(Literal(.7)))) should equal(0.775397496610753 +- 0.00001)
    intercept[CypherTypeException](calc(AsinFunction(Literal("wut"))))
  }

  test("atanTests") {
    asDouble(calc(AtanFunction(Literal(.7)))) should equal(0.610725964389209 +- 0.00001)
    intercept[CypherTypeException](calc(AtanFunction(Literal("wut"))))
  }

  test("atan2Tests") {
    asDouble(calc(Atan2Function(Literal(.7),Literal(.8)))) should equal(0.718829999621624 +- 0.00001)
    asDouble(calc(Atan2Function(Literal(.8),Literal(.8)))) should equal(0.785398163397448 +- 0.00001)
    intercept[CypherTypeException](calc(Atan2Function(Literal("wut"), Literal(.7))))
  }

  test("ceilTests") {
    asDouble(calc(CeilFunction(Literal(.7)))) should equal(1.0 +- 0.00001)
    asDouble(calc(CeilFunction(Literal(-.7)))) should equal(0.0 +- 0.00001)
    intercept[CypherTypeException](calc(CeilFunction(Literal("wut"))))
  }

  test("cosTests") {
    asDouble(calc(CosFunction(Literal(.7)))) should equal(0.764842187284489 +- 0.00001)
    intercept[CypherTypeException](calc(CosFunction(Literal("wut"))))
  }

  test("cotTests") {
    asDouble(calc(CotFunction(Literal(.7)))) should equal(1.18724183212668 +- 0.00001)
    asDouble(calc(CotFunction(Literal(0.0)))) should equal(Double.PositiveInfinity +- 0.00001)
    intercept[CypherTypeException](calc(CotFunction(Literal("wut"))))
  }

  test("degreesTests") {
    asDouble(calc(DegreesFunction(Literal(0.785398163397448)))) should equal(45.0 +- 0.00001)
    asDouble(calc(DegreesFunction(Literal(0.0)))) should equal(0.0 +- 0.00001)
    intercept[CypherTypeException](calc(DegreesFunction(Literal("wut"))))
  }

  test("eTests") {
    asDouble(calc(EFunction())) should equal(2.718281828459045 +- 0.00001)
  }

  test("expTests") {
    asDouble(calc(ExpFunction(Literal(3.2)))) should equal(24.532530197109352 +- 0.00001)
    intercept[CypherTypeException](calc(ExpFunction(Literal("wut"))))
  }

  test("floorTests") {
    asDouble(calc(FloorFunction(Literal(0.9)))) should equal(0.0 +- 0.00001)
    asDouble(calc(FloorFunction(Literal(-0.9)))) should equal(-1.0 +- 0.00001)
    intercept[CypherTypeException](calc(FloorFunction(Literal("wut"))))
  }

  test("logTests") {
    asDouble(calc(LogFunction(Literal(27.0)))) should equal(3.295836866004329 +- 0.00001)
    intercept[CypherTypeException](calc(LogFunction(Literal("wut"))))
  }

  test("log10Tests") {
    asDouble(calc(Log10Function(Literal(27.0)))) should equal(1.4313637641589874 +- 0.00001)
    intercept[CypherTypeException](calc(Log10Function(Literal("wut"))))
  }

  test("piTests") {
    asDouble(calc(PiFunction())) should equal(3.141592653589793 +- 0.00001)
  }

  test("radiansTests") {
    asDouble(calc(RadiansFunction(Literal(45.0)))) should equal(0.785398163397448 +- 0.00001)
    intercept[CypherTypeException](calc(RadiansFunction(Literal("wut"))))
  }

  test("signTests") {
    calc(SignFunction(Literal(-1))) should equal(-1)
    calc(SignFunction(Literal(1))) should equal(1)
    intercept[CypherTypeException](calc(SignFunction(Literal("wut"))))
  }

  test("sinTests") {
    asDouble(calc(SinFunction(Literal(0.7)))) should equal(0.644217687237691 +- 0.00001)
    intercept[CypherTypeException](calc(SinFunction(Literal("wut"))))
  }

  test("tanTests") {
    asDouble(calc(TanFunction(Literal(0.7)))) should equal(0.8422883804630794 +- 0.00001)
    intercept[CypherTypeException](calc(TanFunction(Literal("wut"))))
  }

  test("roundTests") {
    calc(RoundFunction(Literal(1.5))) should equal(2)
    calc(RoundFunction(Literal(12.22))) should equal(12)
    intercept[CypherTypeException](calc(RoundFunction(Literal("wut"))))
  }

  test("powFunction") {
    calc(Pow(Literal(2), Literal(4))) should equal(math.pow(2, 4))
    intercept[CypherTypeException](calc(Pow(Literal("wut"), Literal(2))))
    intercept[CypherTypeException](calc(Pow(Literal(3.1415), Literal("baaaah"))))
  }

  test("sqrtFunction") {
    calc(SqrtFunction(Literal(16))) should equal(4)
    intercept[CypherTypeException](calc(SqrtFunction(Literal("wut"))))
  }

  private def calc(e:Expression) = e(ExecutionContext.empty)(QueryStateHelper.empty)
}

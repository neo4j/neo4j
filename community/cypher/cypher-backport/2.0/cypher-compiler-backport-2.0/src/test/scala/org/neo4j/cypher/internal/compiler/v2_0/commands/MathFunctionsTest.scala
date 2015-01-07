/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions._
import expressions.AbsFunction
import expressions.Literal
import expressions.Pow
import expressions.SignFunction
import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryStateHelper
import org.neo4j.cypher.CypherTypeException
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions

class MathFunctionsTest extends Assertions with NumericHelper {
  @Test def absTests() {
    assert(calc(AbsFunction(Literal(-1))) === 1)
    assert(calc(AbsFunction(Literal(1))) === 1)
    intercept[CypherTypeException](calc(AbsFunction(Literal("wut"))))
  }

  @Test def acosTests() {
    assertEquals(0.795398830184144, asDouble(calc(AcosFunction(Literal(.7)))), 0.00001)
    intercept[CypherTypeException](calc(AcosFunction(Literal("wut"))))
  }

  @Test def asinTests() {
    assertEquals(0.775397496610753, asDouble(calc(AsinFunction(Literal(.7)))), 0.00001)
    intercept[CypherTypeException](calc(AsinFunction(Literal("wut"))))
  }

  @Test def atanTests() {
    assertEquals(0.610725964389209, asDouble(calc(AtanFunction(Literal(.7)))), 0.00001)
    intercept[CypherTypeException](calc(AtanFunction(Literal("wut"))))
  }

  @Test def atan2Tests() {
    assertEquals(0.718829999621624, asDouble(calc(Atan2Function(Literal(.7),Literal(.8)))), 0.00001)
    assertEquals(0.785398163397448, asDouble(calc(Atan2Function(Literal(.8),Literal(.8)))), 0.00001)
    intercept[CypherTypeException](calc(Atan2Function(Literal("wut"), Literal(.7))))
  }

  @Test def ceilTests() {
    assertEquals(1.0, asDouble(calc(CeilFunction(Literal(.7)))), 0.00001)
    assertEquals(0.0, asDouble(calc(CeilFunction(Literal(-.7)))), 0.00001)
    intercept[CypherTypeException](calc(CeilFunction(Literal("wut"))))
  }

  @Test def cosTests() {
    assertEquals(0.764842187284489, asDouble(calc(CosFunction(Literal(.7)))), 0.00001)
    intercept[CypherTypeException](calc(CosFunction(Literal("wut"))))
  }

  @Test def cotTests() {
    assertEquals(1.18724183212668, asDouble(calc(CotFunction(Literal(.7)))), 0.00001)
    assertEquals(Double.PositiveInfinity, asDouble(calc(CotFunction(Literal(0.0)))), 0.00001)
    intercept[CypherTypeException](calc(CotFunction(Literal("wut"))))
  }

  @Test def degreesTests() {
    assertEquals(45.0, asDouble(calc(DegreesFunction(Literal(0.785398163397448)))), 0.00001)
    assertEquals(0.0, asDouble(calc(DegreesFunction(Literal(0.0)))), 0.00001)
    intercept[CypherTypeException](calc(DegreesFunction(Literal("wut"))))
  }

  @Test def eTests() {
    assertEquals(2.718281828459045, asDouble(calc(EFunction())), 0.00001)
  }

  @Test def expTests() {
    assertEquals(24.532530197109352, asDouble(calc(ExpFunction(Literal(3.2)))), 0.00001)
    intercept[CypherTypeException](calc(ExpFunction(Literal("wut"))))
  }

  @Test def floorTests() {
    assertEquals(0.0, asDouble(calc(FloorFunction(Literal(0.9)))), 0.00001)
    assertEquals(-1.0, asDouble(calc(FloorFunction(Literal(-0.9)))), 0.00001)
    intercept[CypherTypeException](calc(FloorFunction(Literal("wut"))))
  }

  @Test def logTests() {
    assertEquals(3.295836866004329, asDouble(calc(LogFunction(Literal(27.0)))), 0.00001)
    intercept[CypherTypeException](calc(LogFunction(Literal("wut"))))
  }

  @Test def log10Tests() {
    assertEquals(1.4313637641589874, asDouble(calc(Log10Function(Literal(27.0)))), 0.00001)
    intercept[CypherTypeException](calc(Log10Function(Literal("wut"))))
  }

  @Test def piTests() {
    assertEquals(3.141592653589793, asDouble(calc(PiFunction())), 0.00001)
  }

  @Test def radiansTests() {
    assertEquals(0.785398163397448, asDouble(calc(RadiansFunction(Literal(45.0)))), 0.00001)
    intercept[CypherTypeException](calc(RadiansFunction(Literal("wut"))))
  }

  @Test def signTests() {
    assert(calc(SignFunction(Literal(-1))) === -1)
    assert(calc(SignFunction(Literal(1))) === 1)
    intercept[CypherTypeException](calc(SignFunction(Literal("wut"))))
  }

  @Test def sinTests() {
    assertEquals(0.644217687237691, asDouble(calc(SinFunction(Literal(0.7)))), 0.00001)
    intercept[CypherTypeException](calc(SinFunction(Literal("wut"))))
  }

  @Test def tanTests() {
    assertEquals(0.8422883804630794, asDouble(calc(TanFunction(Literal(0.7)))), 0.00001)
    intercept[CypherTypeException](calc(TanFunction(Literal("wut"))))
  }

  @Test def roundTests() {
    assert(calc(RoundFunction(Literal(1.5))) === 2)
    assert(calc(RoundFunction(Literal(12.22))) === 12)
    intercept[CypherTypeException](calc(RoundFunction(Literal("wut"))))
  }

  @Test def powFunction() {
    assert(calc(Pow(Literal(2), Literal(4))) === math.pow (2,4))
    intercept[CypherTypeException](calc(Pow(Literal("wut"), Literal(2))))
    intercept[CypherTypeException](calc(Pow(Literal(3.1415), Literal("baaaah"))))
  }

  @Test def sqrtFunction() {
    assert(calc(SqrtFunction(Literal(16))) === 4)
    intercept[CypherTypeException](calc(SqrtFunction(Literal("wut"))))
  }

  private def calc(e:Expression) = e(ExecutionContext.empty)(QueryStateHelper.empty)
}

/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import expressions._
import expressions.AbsFunction
import expressions.Literal
import expressions.Pow
import expressions.SignFunction
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}

class MathFunctionsTest extends Assertions {
  @Test def absTests() {
    assert(calc(AbsFunction(Literal(-1))) === 1)
    assert(calc(AbsFunction(Literal(1))) === 1)
    intercept[CypherTypeException](calc(AbsFunction(Literal("wut"))))
  }

  @Test def signTests() {
    assert(calc(SignFunction(Literal(-1))) === -1)
    assert(calc(SignFunction(Literal(1))) === 1)
    intercept[CypherTypeException](calc(SignFunction(Literal("wut"))))
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
/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.cypher.internal.pipes.ExecutionContext

class MathFunctionsTest extends Assertions {
  @Test def absTests() {
    assert(AbsFunction(Literal(-1))(ExecutionContext.empty) === 1)
    assert(AbsFunction(Literal(1))(ExecutionContext.empty) === 1)
    intercept[CypherTypeException](AbsFunction(Literal("wut"))(ExecutionContext.empty))
  }

  @Test def signTests() {
    assert(SignFunction(Literal(-1))(ExecutionContext.empty) === -1)
    assert(SignFunction(Literal(1))(ExecutionContext.empty) === 1)
    intercept[CypherTypeException](SignFunction(Literal("wut"))(ExecutionContext.empty))
  }

  @Test def roundTests() {
    assert(RoundFunction(Literal(1.5))(ExecutionContext.empty) === 2)
    assert(RoundFunction(Literal(12.22))(ExecutionContext.empty) === 12)
    intercept[CypherTypeException](RoundFunction(Literal("wut"))(ExecutionContext.empty))
  }

  @Test def powFunction() {
    assert(Pow(Literal(2), Literal(4))(ExecutionContext.empty) === math.pow (2,4))
    intercept[CypherTypeException](Pow(Literal("wut"), Literal(2))(ExecutionContext.empty))
    intercept[CypherTypeException](Pow(Literal(3.1415), Literal("baaaah"))(ExecutionContext.empty))
  }

  @Test def sqrtFunction() {
    assert(SqrtFunction(Literal(16))(ExecutionContext.empty) === 4)
    intercept[CypherTypeException](SqrtFunction(Literal("wut"))(ExecutionContext.empty))
  }
}
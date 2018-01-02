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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Literal, Subtract}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SubtractTest extends CypherFunSuite {

  val m = ExecutionContext.empty
  val s = QueryStateHelper.empty


  test("numbers") {
    val expr = Subtract(Literal(2), Literal(1))
   expr(m)(s) should equal(1)
  }

  test("strings") {
    val expr = Subtract(Literal("hello"), Literal("world"))
    intercept[CypherTypeException](expr(m)(s))
  }

  test("stringPlusNumber") {
    val expr = Subtract(Literal("hello"), Literal(1))
    intercept[CypherTypeException](expr(m)(s))
  }

  test("numberPlusString") {
    val expr = Subtract(Literal(1), Literal("world"))
    intercept[CypherTypeException](expr(m)(s))
  }

  test("numberPlusBool") {
    val expr = Subtract(Literal("1"), Literal(true))
    intercept[CypherTypeException](expr(m)(s))
  }
}

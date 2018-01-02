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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Add, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class AddTest extends CypherFunSuite {

  val m = ExecutionContext.empty
  val s = QueryStateHelper.empty

  test("numbers") {
    val expr = Add(Literal(1), Literal(1))
    expr(m)(s) should equal(2)
  }

  test("with_null") {
    val expected = null.asInstanceOf[Any]

    Add(Literal(null), Literal(1))(m)(s) should equal(expected)
    Add(Literal(2), Literal(null))(m)(s) should equal(expected)
  }

  test("strings") {
    val expr = Add(Literal("hello"), Literal("world"))
    expr(m)(s) should equal("helloworld")
  }

  test("stringPlusNumber") {
    val expr = Add(Literal("hello"), Literal(1))
    expr(m)(s) should equal("hello1")
  }

  test("numberPlusString") {
    val expr = Add(Literal(1), Literal("world"))
    expr(m)(s) should equal("1world")
  }

  test("numberPlusBool") {
    val expr = Add(Literal("1"), Literal(true))
    intercept[CypherTypeException](expr(m)(s))
  }
}

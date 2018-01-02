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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class AggregationsAreIsolatedTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = aggregationsAreIsolated

  test("happy when aggregation are top level in expressions") {
    val ast: Expression = CountStar()_

    condition(ast) shouldBe empty
  }

  test("unhappy when aggregation is sub-expression of the expressions") {
    val ast: Expression = Equals(CountStar()_, UnsignedDecimalIntegerLiteral("42")_)_

    condition(ast) should equal(Seq(s"Expression $ast contains child expressions which are aggregations"))
  }

  test("unhappy when aggregations are both top-level and sub-expression of the expression") {
    val equals: Expression = Equals(CountStar()_, UnsignedDecimalIntegerLiteral("42")_)_
    val ast: Expression = FunctionInvocation(FunctionName("count")_, equals)_

    condition(ast) should equal(Seq(s"Expression $equals contains child expressions which are aggregations"))
  }
}

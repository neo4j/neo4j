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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class IsAggregateTest extends CypherFunSuite with AstConstructionTestSupport {

  test("count(*) is an aggregate expression") {
    val expr: Expression = CountStar()_

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("max(null) is an aggregate expression") {
    val expr: Expression = FunctionInvocation(FunctionName("max")_, Null()_)_

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("distinct id(null) an aggregate expression") {
    val expr: Expression = new FunctionInvocation(FunctionName("id")_, distinct = true, Vector(Null()_))(pos)

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("id(null) is not an aggregate expression") {
    val expr: Expression = new FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Null()_))(pos)

    IsAggregate.unapply(expr) should equal(None)
  }

  test("1 is not an aggregate expression") {
    val expr: Expression = SignedDecimalIntegerLiteral("1")_

    IsAggregate.unapply(expr) should equal(None)
  }
}

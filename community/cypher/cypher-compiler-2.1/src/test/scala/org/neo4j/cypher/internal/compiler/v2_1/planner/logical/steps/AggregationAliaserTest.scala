/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.ast.CountStar
import org.neo4j.cypher.internal.compiler.v2_1.functions.{Id, Max}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition

class AggregationAliaserTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should be able to introduce new identifiers for storing the result of the aggregation function") {
    val (projections, aggregations) = aggregationAliaser(Map("x" -> CountStar()_))

    projections should equal(Map("x" -> ident("  AGGREGATION0")))
    aggregations should equal(Map[String, Expression]("  AGGREGATION0" -> CountStar()_))
  }

  test("should be able to introduce new identifiers for storing the result of the aggregation function and preserve the grouping key projections") {
    val (projections, aggregations) = aggregationAliaser(Map("n" -> ident("n"), "x" -> CountStar()_))

    projections should equal(Map("n" -> ident("n"), "x" -> ident("  AGGREGATION0")))
    aggregations should equal(Map[String, Expression]("  AGGREGATION0" -> CountStar()_))
  }

  test("should be able to introduce new identifiers in nested expressions for storing the result of the aggregation function and preserve the grouping key projections") {
    val aggrExpr: Expression = CountStar()_
    val litExpr: Expression = SignedIntegerLiteral("1")_

    val (projections, aggregations) = aggregationAliaser(Map("n" -> ident("n"), "x" -> Add(aggrExpr, litExpr)_))

    projections should equal(Map[String, Expression]("n" -> ident("n"), "x" -> Add(ident("  AGGREGATION0"), litExpr)_))
    aggregations should equal(Map[String, Expression]("  AGGREGATION0" -> aggrExpr))
  }

  test("should be able to introduce new identifiers in nested expressions for storing the result of several aggregation functions and preserve the grouping key projections") {
    val aggrExpr1: Expression = CountStar()(DummyPosition(0))
    val litExpr: Expression = SignedIntegerLiteral("1")_
    val aggrExpr2: Expression = Max.invoke(Id.invoke(ident("n"))(pos))(DummyPosition(1))

    val (projections, aggregations) = aggregationAliaser(Map("n" -> ident("n"), "x" -> Add(aggrExpr1, litExpr)_, "c" -> aggrExpr2))

    projections should equal(Map[String, Expression]("n" -> ident("n"), "x" -> Add(ident("  AGGREGATION0"), litExpr)_, "c" -> ident("  AGGREGATION1")))
    aggregations should equal(Map[String, Expression]("  AGGREGATION0" -> aggrExpr1, "  AGGREGATION1" -> aggrExpr2))
  }
}

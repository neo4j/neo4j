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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.shouldPlanExhaustiveLimit
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val x = literalUnsignedInt(110)
  private val y = literalUnsignedInt(10)

  test("should add skip if query graph contains skip") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(x))
    )

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Skip(startPlan, x))
    context.staticComponents.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(skip = Some(x)))
    )
  }

  test("should add limit if query graph contains limit") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(limit = Some(x))
    )

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Limit(startPlan, x))
    context.staticComponents.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x)))
    )
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(y), limit = Some(x))
    )

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Skip(Limit(startPlan, add(x, y)), y))
    context.staticComponents.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x), skip = Some(y)))
    )
  }

  test("should plan exhaustive limit on top of updating plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .create(createNode("a"))
      .argument()
      .build()
    shouldPlanExhaustiveLimit(plan, None) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(0)) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(1)) should be(true)
  }

  test("should plan exhaustive limit on top of updating plan in RHS") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .subqueryForeach()
      .|.create(createNode("a"))
      .|.argument()
      .argument()
      .build()
    shouldPlanExhaustiveLimit(plan, None) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(0)) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(1)) should be(true)
  }

  test("should plan limit on top of updating plan with Eager plan in between, if limit > 0") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .sort("n ASC")
      .create(createNode("a"))
      .argument()
      .build()
    shouldPlanExhaustiveLimit(plan, None) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(0)) should be(true)
    shouldPlanExhaustiveLimit(plan, Some(1)) should be(false)
  }

  test("should plan limit on top of read-only plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .argument()
      .build()
    shouldPlanExhaustiveLimit(plan, None) should be(false)
    shouldPlanExhaustiveLimit(plan, Some(0)) should be(false)
    shouldPlanExhaustiveLimit(plan, Some(1)) should be(false)
  }

  private def regularProjection(skip: Option[Expression] = None, limit: Option[Expression] = None) =
    RegularQueryProjection(projections = Map(v"n" -> varFor("n")), queryPagination = QueryPagination(skip, limit))

  private def solved(patternNodes: String*): SinglePlannerQuery =
    RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(patternNodes.map(varFor): _*))

  private def queryGraphWith(
    patternNodesInQG: Set[String],
    solved: SinglePlannerQuery,
    projection: QueryProjection,
    interestingOrder: InterestingOrder = InterestingOrder.empty
  ): (RegularSinglePlannerQuery, LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val qg = QueryGraph(patternNodes = patternNodesInQG.map(varFor))
    val query = RegularSinglePlannerQuery(queryGraph = qg, interestingOrder = interestingOrder, horizon = projection)

    val plan = newMockedLogicalPlanWithSolved(
      context.staticComponents.planningAttributes,
      idNames = patternNodesInQG,
      solved = solved
    )

    (query, context, plan)
  }
}

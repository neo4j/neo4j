/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.planner._
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SortSkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val sortVariable: Variable = Variable("n")(pos)
  val columnOrder: ColumnOrder = Ascending("n")
  val projectionsMap: Map[String, Expression] = Map("n" -> sortVariable)

  private val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should add skip if query graph contains skip") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(x)))

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Skip(startPlan, x))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(skip = Some(x))))
  }

  test("should add limit if query graph contains limit") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(limit = Some(x)))

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Limit(startPlan, x, DoNotIncludeTies))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x))))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(y), limit = Some(x)))

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Limit(Skip(startPlan, y), x, DoNotIncludeTies))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x), skip = Some(y))))
  }

  test("should add sort if query graph contains sort items") {
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      interestingOrder = orderBy(sortVariable, projectionsMap))

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(columnOrder)))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortVariable)))
  }

  test("should add sort and pre-projection") {
    // [WITH n, m] WITH n AS n, m AS m, 5 AS notSortColumn ORDER BY m ASCENDING
    val mSortVar = Variable("m")(pos)
    val (query, context, startPlan) = queryGraphWith(
      Set("n", "m"),
      solved("n", "m"),
      // The requirement to sort by m
      interestingOrder = orderBy(mSortVar, Map(mSortVar.name -> mSortVar)),
      projection = regularProjection(projectionsMap = Map(
        // already solved projections
        sortVariable.name -> sortVariable,
        mSortVar.name -> mSortVar,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(mSortVar.name -> mSortVar)))
  }

  test("should add sort and pre-projection for expressions") {
    // [WITH n] WITH n AS n, 5 AS notSortColumn ORDER BY n + 5 ASCENDING
    val sortExpression = Add(sortVariable, SignedDecimalIntegerLiteral("5")(pos))(pos)
    val (query, context, startPlan) = queryGraphWith(
      // The requirement to sort by n + 5
      Set("n"),
      solved("n"),
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))),
      interestingOrder = orderBy(sortExpression, Map(sortVariable.name -> sortVariable))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(Projection(startPlan, Map("n + 5" -> sortExpression)), Seq(Ascending("n + 5"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortVariable)))
  }

  test("should add sort and two step pre-projection for expressions") {
    // [WITH n] WITH n + 10 AS m, 5 AS notSortColumn ORDER BY m + 5 ASCENDING
    val mVar = Variable("m")(pos)
    val mExpr = Add(sortVariable, SignedDecimalIntegerLiteral("10")(pos))(pos)
    val sortExpression = Add(mVar, SignedDecimalIntegerLiteral("5")(pos))(pos)
    val (query, context, startPlan) = queryGraphWith(
      // The requirement to sort by m + 5
      Set("n"),
      solved("n"),
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // a projection necessary for the sorting
        mVar.name -> mExpr,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))),
      interestingOrder = orderBy(sortExpression, Map(mVar.name -> mExpr))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(Projection(Projection(startPlan, Map(mVar.name -> mExpr)), Map("m + 5" -> sortExpression)), Seq(Ascending("m + 5"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(mVar.name -> mExpr)))
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    // [WITH p] WITH p, EXISTS(p.born) AS bday ORDER BY p.name, bday
    val p = Variable("p")(pos)
    val bday = Variable("bday")(pos)
    val pname = Property(p, PropertyKeyName("name")(pos))(pos)
    val bdayExp = FunctionInvocation(Property(p, PropertyKeyName("born")(pos))(pos), FunctionName("exists")(pos))

    val (query, context, startPlan) = queryGraphWith(
      // The requirement to sort by p.name, bday
      patternNodesInQG = Set("p"),
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("p")),
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        p.name -> p,
        // a projection necessary for the sorting
        bday.name -> bdayExp)),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", pname).asc("ignored", bdayExp, Map(bday.name -> bdayExp)))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(
      Sort(
        Projection(
          Projection(startPlan, Map(
            bday.name -> bdayExp)),
          Map("p.name" -> pname)),
        Seq(Ascending("p.name"), Ascending("bday"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(bday.name -> bdayExp)))
  }

  test("should sort first aliased and then unaliased columns in the right order") {
    // [WITH p] WITH p, EXISTS(p.born) AS bday ORDER BY bday, p.name
    val p = Variable("p")(pos)
    val bday = Variable("bday")(pos)
    val pname = Property(p, PropertyKeyName("name")(pos))(pos)
    val bdayExp = FunctionInvocation(Property(p, PropertyKeyName("born")(pos))(pos), FunctionName("exists")(pos))

    val (query, context, startPlan) = queryGraphWith(
      // The requirement to sort by p.name, bday
      patternNodesInQG = Set("p"),
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("p")),
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        p.name -> p,
        // a projection necessary for the sorting
        bday.name -> bdayExp)),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", bdayExp, Map(bday.name -> bdayExp)).asc("ignored", pname))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(
      Sort(
        Projection(
          Projection(startPlan, Map(
            bday.name -> bdayExp)),
          Map("p.name" -> pname)),
        Seq(Ascending("bday"), Ascending("p.name"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(bday.name -> bdayExp)))
  }

  test("should add sort without pre-projection for DistinctQueryProjection") {
    // [WITH DISTINCT n, m] WITH n AS n, m AS m, 5 AS notSortColumn ORDER BY m
    val mSortVar = Variable("m")(pos)

    val projectionsMap = Map(
      sortVariable.name -> sortVariable,
      mSortVar.name -> mSortVar,
      // a projection that sort will not take care of
      "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))
    val projection = DistinctQueryProjection(
      groupingKeys = projectionsMap,
      shuffle = QueryShuffle(skip = None, limit = None)
    )

    val (query, context, startPlan) = queryGraphWith(
      patternNodesInQG = Set("n", "m"),
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"), InterestingOrder.empty, DistinctQueryProjection(Map(mSortVar.name -> mSortVar))),
      projection = projection,
      interestingOrder = orderBy(mSortVar, Map(mSortVar.name -> mSortVar))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(DistinctQueryProjection(Map(mSortVar.name -> mSortVar)))
  }

  test("should add sort without pre-projection for AggregatingQueryProjection") {
    // [WITH n, m, o] // o is an aggregating expression
    // WITH o, n AS n, m AS m, 5 AS notSortColumn ORDER BY m, o
    val mSortVar = Variable("m")(pos)
    val oSortVar = Variable("o")(pos)

    val grouping = Map(
      sortVariable.name -> sortVariable,
      mSortVar.name -> mSortVar,
      // a projection that sort will not take care of
      "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))
    val aggregating = Map(oSortVar.name -> oSortVar)

    val projection = AggregatingQueryProjection(
      groupingExpressions = grouping,
      aggregationExpressions = aggregating,
      shuffle = QueryShuffle(skip = None, limit = None)
    )

    val (query, context, startPlan) = queryGraphWith(
      patternNodesInQG = Set("n", "m", "o"),
      projection = projection,
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"), InterestingOrder.empty, AggregatingQueryProjection(Map(mSortVar.name -> mSortVar), Map(oSortVar.name -> oSortVar))),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", mSortVar, Map(mSortVar.name -> mSortVar)).asc("ignored", oSortVar))
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"), Ascending("o"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(AggregatingQueryProjection(Map(mSortVar.name -> mSortVar), Map(oSortVar.name -> oSortVar)))
  }

  test("should add sort without pre-projection if things are already projected in previous horizon") {
    // [WITH n, m] WITH n AS n, 5 AS notSortColumn ORDER BY m
    val mSortVar = Variable("m")(pos)
    val (query, context, startPlan) = queryGraphWith(
      patternNodesInQG = Set("n", "m"),
      solved = solved("n", "m"),
      // The requirement to sort by m
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))),
      interestingOrder = orderBy(mSortVar)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"))))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty))
  }

  test("should add sort without pre-projection if things are already projected in same horizon") {
    // [WITH n, m] WITH n AS n, m AS m, 5 AS notSortColumn ORDER BY m
    val sortExpression = Add(sortVariable, UnsignedDecimalIntegerLiteral("5")(pos))(pos)
    // given a plan that solves "n"
    //sortVariable
    val (query, context, startPlan) = queryGraphWith(
      // The requirement to sort by n
      patternNodesInQG = Set("n", "m"),
      solved = RegularPlannerQuery(QueryGraph.empty, InterestingOrder.empty, RegularQueryProjection(Map(sortVariable.name -> sortExpression))),
      projection = regularProjection(projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortExpression,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))),
      interestingOrder = orderBy(sortVariable)
    )


    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    result should equal(Sort(startPlan, Seq(columnOrder)))

    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortExpression)))
  }

  test("should add the correct plans when query uses both ORDER BY, SKIP and LIMIT") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set(sortVariable.name),
      solved(),
      projection = regularProjection(skip = Some(y), limit = Some(x)),
      interestingOrder = orderBy(sortVariable)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query, query.interestingOrder, context)

    // then
    val sorted = Sort(startPlan, Seq(columnOrder))
    val skipped = Skip(sorted, y)
    val limited = Limit(skipped, x, DoNotIncludeTies)

    result should equal(limited)
  }

  private def orderBy(sortExpression: Expression, projections: Map[String, Expression] = Map.empty): InterestingOrder =
    InterestingOrder.required(RequiredOrderCandidate.asc("ignored", sortExpression, projections))

  private def regularProjection(skip: Option[Expression] = None, limit: Option[Expression] = None, projectionsMap: Map[String, Expression] = projectionsMap) = {
    val projection = RegularQueryProjection(
      projections = projectionsMap,
      shuffle = QueryShuffle(skip, limit)
    )
    projection
  }

  private def solved(patternNodes: String*): PlannerQuery = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(patternNodes: _*))

  private def queryGraphWith(patternNodesInQG: Set[String],
                             solved: PlannerQuery,
                             projection: QueryProjection = regularProjection(),
                             interestingOrder: InterestingOrder = InterestingOrder.empty):
  (RegularPlannerQuery, LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)

    val qg = QueryGraph(patternNodes = patternNodesInQG)
    val query = RegularPlannerQuery(queryGraph = qg, horizon = projection, interestingOrder = interestingOrder)

    val plan = newMockedLogicalPlanWithSolved(context.planningAttributes, idNames = patternNodesInQG, solved = solved)

    (query, context, plan)
  }
}

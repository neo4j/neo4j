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
///**
// * Copyright (c) 2002-2012 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.executionplan.builders
//
//import org.neo4j.cypher.internal.commands.{AggregationExpression, SortItem, Expression}
//import org.neo4j.cypher.internal.pipes.{OrderedAggregationPipe, SortPipe, ExtractPipe}
//import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
//
//class SortedAggregationBuilder extends PlanBuilder with ExpressionExtractor {
//  def apply(plan: ExecutionPlanInProgress) = {
//    val q = plan.query
//    val p = plan.pipe
//
//    val sortExpressions = q.sort.filter(_.unsolved).map(_.token.expression)
//    val sortItems = q.sort.filter(_.unsolved).map(_.token)
//    val (keyExpressions, aggregationExpressions) = getKeysAndAggregates(plan)
//
//    val extractPipe = new ExtractPipe(p, keyExpressions)
//
//    val keyColumnsNotAlreadySorted = keyExpressions.
//      filterNot(sortExpressions.contains).
//      map(SortItem(_, true))
//
//    val sortPipe = new SortPipe(extractPipe, (sortItems ++ keyColumnsNotAlreadySorted).toList)
//    val aggregationPipe = new OrderedAggregationPipe(sortPipe, keyExpressions, aggregationExpressions.map(_.asInstanceOf[AggregationExpression]))
//
//    val resultQ = q.copy(
//      aggregation = q.aggregation.map(_.solve),
//      aggregateQuery = q.aggregateQuery.solve,
//      sort = q.sort.map(_.solve),
//      extracted = true
//    )
//
//    plan.copy(query = resultQ, pipe = aggregationPipe)
//  }
//
//
//  def canWorkWith(plan: ExecutionPlanInProgress) = {
//    val q = plan.query
//
//    if (!q.readyToAggregate || q.aggregation.isEmpty)
//      false //If other things are still to do, let's deny
//    else {
//      val (keyExpressions, _) = getKeysAndAggregates(plan)
//
//      val sortExpressions = q.sort.filter(_.unsolved).map(_.token.expression)
//      sortExpressions.nonEmpty && canUseOrderedAggregation(sortExpressions, keyExpressions)
//    }
//  }
//
//  private def canUseOrderedAggregation(sortExpressions: Seq[Expression], keyExpressions: Seq[Expression]): Boolean = keyExpressions.take(sortExpressions.size) == sortExpressions
//
//  def priority: Int = 0
//}
//

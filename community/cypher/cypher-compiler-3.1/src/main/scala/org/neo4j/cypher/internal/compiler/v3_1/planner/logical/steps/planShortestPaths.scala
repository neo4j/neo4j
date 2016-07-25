/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v3_1.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{Ascending, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.idp.expandSolverStep
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{IdName, LogicalPlan, ShortestPathPattern, _}
import org.neo4j.cypher.internal.compiler.v3_1.planner.{Predicate, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_1.notification.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.frontend.v3_1.{ExhaustiveShortestPathForbiddenException, InternalException}
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.ast.functions.{Nodes, Length}

case object planShortestPaths {

  def apply(inner: LogicalPlan, queryGraph: QueryGraph, shortestPaths: ShortestPathPattern)
           (implicit context: LogicalPlanningContext): LogicalPlan = {

    val variables = Set(shortestPaths.name, Some(shortestPaths.rel.name)).flatten
    val predicates = queryGraph.selections.predicates.collect {
      case Predicate(dependencies, expr: Expression) if (dependencies intersect variables).nonEmpty => expr
    }.toSeq

    def doesNotDependOnFullPath(predicate: Expression): Boolean = {
      (predicate.dependencies.map(IdName.fromVariable) intersect variables).isEmpty
    }

    val (safePredicates, needFallbackPredicates) = predicates.partition {
      // TODO: Once we support node predicates we should enable all NONE and ALL predicates as safe predicates
      case NoneIterablePredicate(_, f@FunctionInvocation(_, _, _)) if f.function contains Nodes => false
      case AllIterablePredicate(_, f@FunctionInvocation(_, _, _)) if f.function contains Nodes => false
      case NoneIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) => true
      case AllIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) => true
      case _ => false
    }

    if (needFallbackPredicates.nonEmpty) {
      planShortestPathsWithFallback(inner, shortestPaths, predicates, safePredicates, needFallbackPredicates, queryGraph)
    }
    else {
      context.logicalPlanProducer.planShortestPath(inner, shortestPaths, predicates)
    }
  }

  private def createPathExpression(pattern: PatternElement): PathExpression = {
    val pos = pattern.position
    val path = EveryPath(pattern)
    val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
    PathExpression(step)(pos)
  }

  private def planShortestPathsWithFallback(inner: LogicalPlan, shortestPath: ShortestPathPattern,
                                            predicates: Seq[Expression],
                                            safePredicates: Seq[Expression],
                                            unsafePredicates: Seq[Expression],
                                            queryGraph: QueryGraph)
                                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    // create warning for planning a shortest path fallback
    context.notificationLogger += ExhaustiveShortestPathForbiddenNotification(shortestPath.expr.position)

    val lpp = context.logicalPlanProducer

    // Plan FindShortestPaths within an Apply with an Optional so we get null rows when
    // the graph algorithm does not find anything (left-hand-side)
    val lhsArgument = lpp.planArgumentRowFrom(inner)
    val lhsSp = lpp.planShortestPath(lhsArgument, shortestPath, predicates)
    val lhsOption = lpp.planOptional(lhsSp, Set.empty)
    val lhs = lpp.planApply(inner, lhsOption)

    val rhsArgument = lpp.planArgumentRowFrom(lhs)

    val rhs = if (context.errorIfShortestPathFallbackUsedAtRuntime) {
      lpp.planError(rhsArgument, new ExhaustiveShortestPathForbiddenException)
    } else {
      buildPlanShortestPathsFallbackPlans(shortestPath, rhsArgument, predicates, queryGraph)
    }

    // We have to force the plan to solve what we actually solve
    val solved = lpp.estimatePlannerQuery(inner.solved.amendQueryGraph(_.addShortestPath(shortestPath)
      .addPredicates(predicates: _*)))

    lpp.planAntiConditionalApply(lhs, rhs, Seq(shortestPath.name.get)).updateSolved(solved)
  }

  private def buildPlanShortestPathsFallbackPlans(shortestPath: ShortestPathPattern, rhsArgument: LogicalPlan,
                                                  predicates: Seq[Expression], queryGraph: QueryGraph)
                                                 (implicit context: LogicalPlanningContext): LogicalPlan = {
    // TODO: Decide the best from and to based on degree (generate two alternative plans and let planner decide)
    // (or do bidirectional var length expand)
    val pattern = shortestPath.rel
    val from = pattern.left
    val lpp = context.logicalPlanProducer

    // We assume there is always a path name (either explicit or auto-generated)
    val pathName = shortestPath.name.get

    // Plan a fallback branch using VarExpand(Into) (right-hand-side)
    val rhsVarExpand = expandSolverStep.planSinglePatternSide(queryGraph, pattern, rhsArgument, from)
      .getOrElse(throw new InternalException("Expected the nodes needed for this expansion to exist"))

    // Projection with path
    val map = Map(pathName.name -> createPathExpression(shortestPath.expr.element))
    val rhsProjection = lpp.planRegularProjection(rhsVarExpand, map, map)

    // Filter using predicates
    val rhsFiltered = context.logicalPlanProducer.planSelection(rhsProjection, predicates, predicates)

    // Plan Sort and Limit
    val pos = shortestPath.expr.position
    val pathVariable = Variable(pathName.name)(pos)
    val lengthOfPath = FunctionInvocation(FunctionName(Length.name)(pos), pathVariable)(pos)
    val columnName = FreshIdNameGenerator.name(pos)

    val rhsProjMap = Map(columnName -> lengthOfPath)
    val rhsProjected = lpp.planRegularProjection(rhsFiltered, rhsProjMap, rhsProjMap)
    val sortDescription = Seq(Ascending(IdName(columnName)))
    val sorted = lpp.planSort(rhsProjected, sortDescription, Seq.empty)
    val ties = if (shortestPath.single) DoNotIncludeTies else IncludeTies
    lpp.planLimit(sorted, SignedDecimalIntegerLiteral("1")(pos), ties)
  }
}

/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException
import org.neo4j.exceptions.InternalException

case object planShortestPaths {

  def apply(inner: LogicalPlan,
            queryGraph: QueryGraph,
            shortestPaths: ShortestPathPattern,
            context: LogicalPlanningContext): LogicalPlan = {

    val variables = Set(shortestPaths.name, Some(shortestPaths.rel.name)).flatten
    def predicateAppliesToShortestPath(p: Predicate) =
    // only select predicates related to this pattern (this is code in common with normal MATCH Pattern clauses)
      p.hasDependenciesMet(variables ++ inner.availableSymbols) &&
        // And filter with predicates that explicitly depend on shortestPath variables
        (p.dependencies intersect variables).nonEmpty

    val predicates = queryGraph.selections.predicates.collect {
      case p@Predicate(_, expr) if predicateAppliesToShortestPath(p) => expr
    }.toIndexedSeq

    def doesNotDependOnFullPath(predicate: Expression): Boolean = {
      (predicate.dependencies.map(_.name) intersect variables).isEmpty
    }

    val (_, needFallbackPredicates) = predicates.partition {
      case NoneIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) => true
      case AllIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) => true
      case _ => false
    }

    if (needFallbackPredicates.nonEmpty) {
      planShortestPathsWithFallback(inner, shortestPaths, predicates, queryGraph, context)
    }
    else {
      context.logicalPlanProducer.planShortestPath(inner, shortestPaths, predicates, withFallBack = false,
        disallowSameNode = context.errorIfShortestPathHasCommonNodesAtRuntime, context = context)
    }
  }

  private def createPathExpression(pattern: PatternElement): PathExpression = {
    val pos = pattern.position
    val path = EveryPath(pattern)
    val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
    PathExpression(step)(pos)
  }

  private def planShortestPathsWithFallback(inner: LogicalPlan,
                                            shortestPath: ShortestPathPattern,
                                            predicates: Seq[Expression],
                                            queryGraph: QueryGraph,
                                            context: LogicalPlanningContext) = {
    // create warning for planning a shortest path fallback
    context.notificationLogger.log(ExhaustiveShortestPathForbiddenNotification(shortestPath.expr.position))

    val lpp = context.logicalPlanProducer

    // Plan FindShortestPaths within an Apply with an Optional so we get null rows when
    // the graph algorithm does not find anything (left-hand-side)
    val lhsArgument = context.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestPath.rel.nodes._1, shortestPath.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context)

    val lhsSp = lpp.planShortestPath(lhsArgument, shortestPath, predicates, withFallBack = true,
      disallowSameNode = context.errorIfShortestPathHasCommonNodesAtRuntime, context = context)
    val lhsOption = lpp.planOptional(lhsSp, lhsArgument.availableSymbols, context, QueryGraph.empty)
    val lhs = lpp.planApply(inner, lhsOption, context)

    val rhsArgument = context.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestPath.rel.nodes._1, shortestPath.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context)

    val rhs = if (context.errorIfShortestPathFallbackUsedAtRuntime) {
      lpp.planError(rhsArgument, new ExhaustiveShortestPathForbiddenException, context)
    } else {
      buildPlanShortestPathsFallbackPlans(shortestPath, rhsArgument, predicates, queryGraph, context)
    }

    // We have to force the plan to solve what we actually solve
    val solved = context.planningAttributes.solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addShortestPath(shortestPath).addPredicates(predicates: _*))

    lpp.planAntiConditionalApply(lhs, rhs, Seq(shortestPath.name.get), context, Some(solved))
  }

  private def buildPlanShortestPathsFallbackPlans(shortestPath: ShortestPathPattern,
                                                  rhsArgument: LogicalPlan,
                                                  predicates: Seq[Expression],
                                                  queryGraph: QueryGraph,
                                                  context: LogicalPlanningContext): LogicalPlan = {
    // TODO: Decide the best from and to based on degree (generate two alternative plans and let planner decide)
    // (or do bidirectional var length expand)
    val pattern = shortestPath.rel
    val from = pattern.left
    val lpp = context.logicalPlanProducer

    // We assume there is always a path name (either explicit or auto-generated)
    val pathName = shortestPath.name.get

    // Plan a fallback branch using VarExpand(Into) (right-hand-side)
    val rhsVarExpand = expandSolverStep.planSinglePatternSide(queryGraph, pattern, rhsArgument, from, context)
      .getOrElse(throw new InternalException("Expected the nodes needed for this expansion to exist"))

    // Projection with path
    val map = Map(pathName -> createPathExpression(shortestPath.expr.element))
    val rhsProjection = lpp.planRegularProjection(rhsVarExpand, map, Some(map), context)

    // Filter using predicates
    val rhsFiltered = context.logicalPlanProducer.planSelection(rhsProjection, predicates, context)

    // Plan Top
    val pos = shortestPath.expr.position
    val pathVariable = Variable(pathName)(pos)
    val lengthOfPath = FunctionInvocation(FunctionName(Length.name)(pos), pathVariable)(pos)
    val columnName = context.anonymousVariableNameGenerator.nextName

    val rhsProjMap = Map(columnName -> lengthOfPath)
    val rhsProjected = lpp.planRegularProjection(rhsFiltered, rhsProjMap, Some(rhsProjMap), context)
    val sortDescription = Seq(Ascending(columnName))
    val plan = if (shortestPath.single) {
      lpp.planTop(rhsProjected, SignedDecimalIntegerLiteral("1")(pos), sortDescription, Seq.empty, InterestingOrder.empty, context)
    } else {
      lpp.planTop1WithTies(rhsProjected, sortDescription, Seq.empty, InterestingOrder.empty, context)
    }

    // Even though we don't use ProvidedOrder or Interesting order, since we don't affect other parts of the planning here
    // we can still set leveragedOrder to true, correctly.
    context.planningAttributes.leveragedOrders.set(plan.id, true)
    plan
  }
}

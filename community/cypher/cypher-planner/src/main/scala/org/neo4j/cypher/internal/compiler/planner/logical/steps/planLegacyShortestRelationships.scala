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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException

case object planLegacyShortestRelationships {

  def apply(
    inner: LogicalPlan,
    queryGraph: QueryGraph,
    shortestRelationship: ShortestRelationshipPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val variables = Set(shortestRelationship.name, Some(shortestRelationship.rel.name)).flatten
    def predicateAppliesToShortestRelationship(p: Predicate) =
      // only select predicates related to this pattern (this is code in common with normal MATCH Pattern clauses)
      p.hasDependenciesMet(variables ++ inner.availableSymbols.map(_.name)) &&
        // And filter with predicates that explicitly depend on shortestPath variables
        (p.dependencies intersect variables).nonEmpty

    val predicates = queryGraph.selections.predicates.collect {
      case p @ Predicate(_, expr) if predicateAppliesToShortestRelationship(p) => expr
    }.toIndexedSeq

    def doesNotDependOnFullPath(predicate: Expression): Boolean = {
      (predicate.dependencies.map(_.name) intersect variables).isEmpty
    }

    val (_, needFallbackPredicates) = predicates.partition {
      case NoneIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) =>
        true
      case AllIterablePredicate(FilterScope(_, Some(innerPredicate)), _) if doesNotDependOnFullPath(innerPredicate) =>
        true
      case _ => false
    }

    if (needFallbackPredicates.nonEmpty) {
      planLegacyShortestRelationshipsWithFallback(
        inner,
        shortestRelationship,
        predicates,
        queryGraph,
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planLegacyShortestRelationship(
        inner,
        shortestRelationship,
        predicates,
        withFallBack = false,
        disallowSameNode = context.settings.errorIfShortestPathHasCommonNodesAtRuntime,
        context = context
      )
    }
  }

  private def createPathExpression(pattern: PatternElement): PathExpression = {
    val pos = pattern.position
    val path = PatternPart(pattern)
    val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
    PathExpression(step)(pos)
  }

  private def planLegacyShortestRelationshipsWithFallback(
    inner: LogicalPlan,
    shortestRelationship: ShortestRelationshipPattern,
    predicates: Seq[Expression],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ) = {
    // create warning for planning a shortest path fallback
    context.staticComponents.notificationLogger.log(
      ExhaustiveShortestPathForbiddenNotification(shortestRelationship.expr.position)
    )

    val lpp = context.staticComponents.logicalPlanProducer

    // Plan FindShortestPaths within an Apply with an Optional so we get null rows when
    // the graph algorithm does not find anything (left-hand-side)
    val lhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestRelationship.rel.nodes._1, shortestRelationship.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context
    )

    val lhsSp = lpp.planLegacyShortestRelationship(
      lhsArgument,
      shortestRelationship,
      predicates,
      withFallBack = true,
      disallowSameNode = context.settings.errorIfShortestPathHasCommonNodesAtRuntime,
      context = context
    )
    val lhsOption = lpp.planOptional(lhsSp, lhsArgument.availableSymbols.map(_.name), context, QueryGraph.empty)
    val lhs = lpp.planApply(inner, lhsOption, context)

    val rhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestRelationship.rel.nodes._1, shortestRelationship.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context
    )

    val rhs =
      if (context.settings.errorIfShortestPathFallbackUsedAtRuntime) {
        lpp.planError(rhsArgument, new ExhaustiveShortestPathForbiddenException, context)
      } else {
        buildPlanLegacyShortestRelationshipsFallbackPlans(
          shortestRelationship,
          rhsArgument,
          predicates,
          queryGraph,
          context
        )
      }

    // We have to force the plan to solve what we actually solve
    val solved = context.staticComponents.planningAttributes.solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addShortestRelationship(shortestRelationship).addPredicates(predicates: _*)
    )

    lpp.planAntiConditionalApply(lhs, rhs, Seq(shortestRelationship.name.get), context, Some(solved))
  }

  private def buildPlanLegacyShortestRelationshipsFallbackPlans(
    shortestRelationship: ShortestRelationshipPattern,
    rhsArgument: LogicalPlan,
    predicates: Seq[Expression],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // TODO: Decide the best from and to based on degree (generate two alternative plans and let planner decide)
    // (or do bidirectional var length expand)
    val pattern = shortestRelationship.rel
    val from = pattern.left
    val lpp = context.staticComponents.logicalPlanProducer

    // We assume there is always a path name (either explicit or auto-generated)
    val pathName = shortestRelationship.name.get

    // Plan a fallback branch using VarExpand(Into) (right-hand-side)
    val rhsVarExpand =
      expandSolverStep.produceExpandLogicalPlan(
        queryGraph,
        pattern,
        pattern.name,
        rhsArgument,
        from,
        rhsArgument.availableSymbols.map(_.name),
        context
      )

    // Projection with path
    val map = Map(pathName -> createPathExpression(shortestRelationship.expr.element))
    val rhsProjection = lpp.planRegularProjection(rhsVarExpand, Set.empty, map, Some(map), context)

    // Filter using predicates
    val rhsFiltered = context.staticComponents.logicalPlanProducer.planSelection(rhsProjection, predicates, context)

    // Plan Top
    val pos = shortestRelationship.expr.position
    val pathVariable = Variable(pathName)(pos)
    val lengthOfPath = FunctionInvocation(FunctionName(Length.name)(pos), pathVariable)(pos)
    val columnName = context.staticComponents.anonymousVariableNameGenerator.nextName

    val rhsProjMap = Map(columnName -> lengthOfPath)
    val rhsProjected = lpp.planRegularProjection(rhsFiltered, Set.empty, rhsProjMap, Some(rhsProjMap), context)
    val sortDescription = Seq(Ascending(varFor(columnName)))
    val plan =
      if (shortestRelationship.single) {
        lpp.planTop(
          rhsProjected,
          SignedDecimalIntegerLiteral("1")(pos),
          sortDescription,
          Seq.empty,
          InterestingOrder.empty,
          context
        )
      } else {
        lpp.planTop1WithTies(rhsProjected, sortDescription, Seq.empty, InterestingOrder.empty, context)
      }

    // Even though we don't use ProvidedOrder or Interesting order, since we don't affect other parts of the planning here
    // we can still set leveragedOrder to true, correctly.
    context.staticComponents.planningAttributes.leveragedOrders.set(plan.id, true)
    plan
  }
}

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
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractShortestPathPredicates
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException

case object planShortestPaths {

  def apply(
    inner: LogicalPlan,
    queryGraph: QueryGraph,
    shortestPaths: ShortestPathPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val patternRelationship = shortestPaths.rel
    val relName = patternRelationship.name
    val pathNameOpt = shortestPaths.name

    val variables = Set(shortestPaths.name, Some(shortestPaths.rel.name)).flatten

    def predicateAppliesToShortestPath(p: Predicate) =
      // only select predicates related to this pattern (this is code in common with normal MATCH Pattern clauses)
      p.hasDependenciesMet(variables ++ inner.availableSymbols) &&
        // And filter with predicates that explicitly depend on shortestPath variables
        (p.dependencies intersect variables).nonEmpty

    // The predicates which apply to the shortest path pattern will be solved by this operator
    val solvedPredicates = queryGraph.selections.predicates.collect {
      case p @ Predicate(_, expr) if predicateAppliesToShortestPath(p) => expr
    }

    val (
      nodePredicates: Set[VariablePredicate],
      relPredicates: Set[VariablePredicate],
      nonExtractedPerStepPredicates: Set[Expression]
    ) =
      extractShortestPathPredicates(solvedPredicates, pathNameOpt, Some(relName))

    val pathPredicates = solvedPredicates.diff(nonExtractedPerStepPredicates)

    if (pathPredicates.nonEmpty) {
      planShortestPathsWithFallback(
        inner,
        shortestPaths,
        nodePredicates,
        relPredicates,
        pathPredicates,
        solvedPredicates,
        queryGraph,
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planShortestPath(
        inner,
        shortestPaths,
        nodePredicates,
        relPredicates,
        pathPredicates,
        solvedPredicates,
        withFallBack = false,
        disallowSameNode = context.settings.errorIfShortestPathHasCommonNodesAtRuntime,
        context = context
      )
    }
  }

  private def createPathExpression(pattern: PatternElement): PathExpression = {
    val pos = pattern.position
    val path = EveryPath(pattern)
    val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
    PathExpression(step)(pos)
  }

  private def planShortestPathsWithFallback(
    inner: LogicalPlan,
    shortestPath: ShortestPathPattern,
    nodePredicates: Set[VariablePredicate],
    relPredicates: Set[VariablePredicate],
    pathPredicates: Set[Expression],
    solvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ) = {
    // create warning for planning a shortest path fallback
    context.staticComponents.notificationLogger.log(
      ExhaustiveShortestPathForbiddenNotification(shortestPath.expr.position)
    )

    val lpp = context.staticComponents.logicalPlanProducer

    // Plan FindShortestPaths within an Apply with an Optional so we get null rows when
    // the graph algorithm does not find anything (left-hand-side)
    val lhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestPath.rel.nodes._1, shortestPath.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context
    )

    val lhsSp = lpp.planShortestPath(
      lhsArgument,
      shortestPath,
      nodePredicates,
      relPredicates,
      pathPredicates,
      solvedPredicates,
      withFallBack = true,
      disallowSameNode = context.settings.errorIfShortestPathHasCommonNodesAtRuntime,
      context = context
    )
    val lhsOption = lpp.planOptional(lhsSp, lhsArgument.availableSymbols, context, QueryGraph.empty)
    val lhs = lpp.planApply(inner, lhsOption, context)

    val rhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = Set(shortestPath.rel.nodes._1, shortestPath.rel.nodes._2),
      patternRels = Set.empty,
      other = Set.empty,
      context = context
    )

    val rhs =
      if (context.settings.errorIfShortestPathFallbackUsedAtRuntime) {
        lpp.planError(rhsArgument, new ExhaustiveShortestPathForbiddenException, context)
      } else {
        buildPlanShortestPathsFallbackPlans(shortestPath, rhsArgument, solvedPredicates.toSeq, queryGraph, context)
      }

    // We have to force the plan to solve what we actually solve
    val solved = context.staticComponents.planningAttributes.solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addShortestPath(shortestPath).addPredicates(solvedPredicates.toSeq: _*)
    )

    lpp.planAntiConditionalApply(lhs, rhs, Seq(shortestPath.name.get), context, Some(solved))
  }

  private def buildPlanShortestPathsFallbackPlans(
    shortestPath: ShortestPathPattern,
    rhsArgument: LogicalPlan,
    predicates: Seq[Expression],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // TODO: Decide the best from and to based on degree (generate two alternative plans and let planner decide)
    // (or do bidirectional var length expand)
    val pattern = shortestPath.rel
    val from = pattern.left
    val lpp = context.staticComponents.logicalPlanProducer
    // We assume there is always a path name (either explicit or auto-generated)
    val pathName = shortestPath.name.get

    // TODO: When the path is named, we need to redo the projectNamedPaths stuff so that
    // we can extract the per step predicates again

    // Projection with path
    val map = Map(pathName -> createPathExpression(shortestPath.expr.element))

    // Rewriter for path name to path expression
    val rewriter = topDown(Rewriter.lift {
      case Variable(name) if name == pathName => createPathExpression(shortestPath.expr.element)
    })

    // Rewrite query graph to match during inlining of predicates in var expand
    val rewrittenQg = queryGraph.withSelections {
      val rewrittenExpressions = queryGraph.selections.predicates.map { predicate =>
        predicate.expr.endoRewrite(rewriter)
      }
      Selections.from(rewrittenExpressions)
    }

    // Plan a fallback branch using VarExpand(Into) (right-hand-side)
    val rhsVarExpand =
      expandSolverStep.produceExpandLogicalPlan(
        rewrittenQg,
        pattern,
        pattern.name,
        rhsArgument,
        from,
        rhsArgument.availableSymbols,
        context
      )

    // Expressions solved in var expand
    val varExpandSolvedExpr =
      context.staticComponents.planningAttributes.solveds.get(
        rhsVarExpand.id
      ).asSinglePlannerQuery.lastQueryGraph.selections.predicates

    val rhsProjection = lpp.planRegularProjection(rhsVarExpand, Set.empty, map, Some(map), context)

    // Filter out predicates solved in var expand
    val filteredPredicates =
      predicates.filterNot(predicate => varExpandSolvedExpr.map(_.expr).contains(predicate.endoRewrite(rewriter)))

    // Filter using filtered predicates
    val rhsFiltered =
      context.staticComponents.logicalPlanProducer.planSelection(rhsProjection, filteredPredicates, context)

    // Plan Top
    val pos = shortestPath.expr.position
    val pathVariable = Variable(pathName)(pos)
    val lengthOfPath = FunctionInvocation(FunctionName(Length.name)(pos), pathVariable)(pos)
    val columnName = context.staticComponents.anonymousVariableNameGenerator.nextName

    val rhsProjMap = Map(columnName -> lengthOfPath)
    val rhsProjected = lpp.planRegularProjection(rhsFiltered, Set.empty, rhsProjMap, Some(rhsProjMap), context)
    val sortDescription = Seq(Ascending(columnName))
    val plan =
      if (shortestPath.single) {
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

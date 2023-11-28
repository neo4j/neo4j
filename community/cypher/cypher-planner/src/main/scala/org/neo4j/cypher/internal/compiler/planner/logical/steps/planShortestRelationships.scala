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

import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractShortestPathPredicates
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
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
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException

case object planShortestRelationships {

  private val prettifier = Prettifier(QueryGraph.stringifier)

  def apply(
    inner: LogicalPlan,
    queryGraph: QueryGraph,
    shortestRelationship: ShortestRelationshipPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val patternRelationship = shortestRelationship.rel
    val relName = patternRelationship.variable.name
    val pathNameOpt = shortestRelationship.maybePathVar.map(_.name)

    val variables = Set(shortestRelationship.maybePathVar, Some(shortestRelationship.rel.variable)).flatten.map(_.name)

    def predicateAppliesToShortestRelationship(p: Predicate) =
      // only select predicates related to this pattern (this is code in common with normal MATCH Pattern clauses)
      p.hasDependenciesMet(variables ++ inner.availableSymbols.map(_.name)) &&
        // And filter with predicates that explicitly depend on shortestPath variables
        (p.dependencies intersect variables).nonEmpty

    // The predicates which apply to the shortest path pattern will be solved by this operator
    val solvedPredicates = queryGraph.selections.predicates.collect {
      case p @ Predicate(_, expr) if predicateAppliesToShortestRelationship(p) => expr
    }

    val (
      nodePredicates: Set[VariablePredicate],
      relPredicates: Set[VariablePredicate],
      nonExtractedPerStepPredicates: Set[Expression]
    ) =
      extractShortestPathPredicates(solvedPredicates, pathNameOpt, Some(relName))

    val pathPredicates = solvedPredicates.diff(nonExtractedPerStepPredicates)

    if (pathPredicates.nonEmpty) {
      planShortestRelationshipsWithFallback(
        inner,
        shortestRelationship,
        nodePredicates,
        relPredicates,
        pathPredicates,
        solvedPredicates,
        queryGraph,
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planShortestRelationship(
        inner,
        shortestRelationship,
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
    val path = PatternPart(pattern)
    val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
    PathExpression(step)(pos)
  }

  private def planShortestRelationshipsWithFallback(
    inner: LogicalPlan,
    shortestRelationship: ShortestRelationshipPattern,
    nodePredicates: Set[VariablePredicate],
    relPredicates: Set[VariablePredicate],
    pathPredicates: Set[Expression],
    solvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ) = {
    // create warning for planning a shortest path fallback
    val prettyPathPredicates = pathPredicates.map(prettifier.expr.apply)
    context.staticComponents.notificationLogger.log(
      ExhaustiveShortestPathForbiddenNotification(shortestRelationship.expr.position, prettyPathPredicates)
    )

    val lpp = context.staticComponents.logicalPlanProducer

    val argumentNodes = shortestRelationship.rel.boundaryNodesSet.map(_.name)
    val otherDependencies =
      pathPredicates.flatMap(_.dependencies.map(_.name)) -- shortestRelationship.maybePathVar.map(_.name) ++
        Set(nodePredicates, relPredicates).flatMap(_.flatMap { varPred =>
          varPred.predicate.dependencies.map(_.name) - varPred.variable.name
        })
    // Plan FindShortestPaths within an Apply with an Optional so we get null rows when
    // the graph algorithm does not find anything (left-hand-side)
    val lhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = argumentNodes,
      patternRels = Set.empty,
      other = otherDependencies,
      context = context
    )

    val lhsSp = lpp.planShortestRelationship(
      lhsArgument,
      shortestRelationship,
      nodePredicates,
      relPredicates,
      pathPredicates,
      solvedPredicates,
      withFallBack = true,
      disallowSameNode = context.settings.errorIfShortestPathHasCommonNodesAtRuntime,
      context = context
    )
    val lhsOption = lpp.planOptional(lhsSp, lhsArgument.availableSymbols.map(_.name), context, QueryGraph.empty)
    val lhs = lpp.planApply(inner, lhsOption, context)

    val rhsArgument = context.staticComponents.logicalPlanProducer.planArgument(
      patternNodes = argumentNodes,
      patternRels = Set.empty,
      other = otherDependencies,
      context = context
    )

    val rhs =
      if (context.settings.errorIfShortestPathFallbackUsedAtRuntime) {
        lpp.planError(rhsArgument, new ExhaustiveShortestPathForbiddenException, context)
      } else {
        buildPlanShortestRelationshipsFallbackPlans(
          shortestRelationship,
          rhsArgument,
          solvedPredicates.toSeq,
          queryGraph,
          context
        )
      }

    // We have to force the plan to solve what we actually solve
    val solved = context.staticComponents.planningAttributes.solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addShortestRelationship(shortestRelationship).addPredicates(solvedPredicates.toSeq: _*)
    )

    lpp.planAntiConditionalApply(lhs, rhs, Seq(shortestRelationship.maybePathVar.get.name), context, Some(solved))
  }

  private def buildPlanShortestRelationshipsFallbackPlans(
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
    val pathVariable = shortestRelationship.maybePathVar.get

    // TODO: When the path is named, we need to redo the projectNamedPaths stuff so that
    // we can extract the per step predicates again

    // Projection with path
    val map = Map(pathVariable.name -> createPathExpression(shortestRelationship.expr.element))

    // Rewriter for path name to path expression
    val rewriter = topDown(Rewriter.lift {
      case Variable(name) if name == pathVariable.name => createPathExpression(shortestRelationship.expr.element)
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
        pattern.variable.name,
        rhsArgument,
        from.name,
        rhsArgument.availableSymbols.map(_.name),
        context
      )

    // Expressions solved in var expand
    val varExpandSolvedExpr =
      context.staticComponents.planningAttributes.solveds.get(
        rhsVarExpand.id
      ).asSinglePlannerQuery.lastQueryGraph.selections.predicates

    val rhsProjection = lpp.planRegularProjection(rhsVarExpand, map, Some(map), context)

    // Filter out predicates solved in var expand
    val filteredPredicates =
      predicates.filterNot(predicate => varExpandSolvedExpr.map(_.expr).contains(predicate.endoRewrite(rewriter)))

    // Filter using filtered predicates
    val rhsFiltered =
      context.staticComponents.logicalPlanProducer.planSelection(rhsProjection, filteredPredicates, context)

    // Plan Top
    val pos = shortestRelationship.expr.position
    val lengthOfPath = FunctionInvocation(FunctionName(Length.name)(pos), pathVariable)(pos)
    val columnName = context.staticComponents.anonymousVariableNameGenerator.nextName

    val rhsProjMap = Map(columnName -> lengthOfPath)
    val rhsProjected = lpp.planRegularProjection(rhsFiltered, rhsProjMap, Some(rhsProjMap), context)
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

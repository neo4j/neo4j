/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.opencypher.v9_0.util.{FreshIdNameGenerator, Rewriter, UnNamedNameGenerator, topDown}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{LogicalPlanningContext, patternExpressionRewriter}
import org.opencypher.v9_0.rewriting.rewriters.{PatternExpressionPatternElementNamer, projectNamedPaths}
import org.neo4j.cypher.internal.ir.v3_5.QueryGraph
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Exists

import scala.collection.mutable
import scala.reflect.ClassTag

/*
Prepares expressions containing pattern expressions by solving them in a sub-query through RollUpApply and replacing
the original expression with an identifier, or preferably GetDegree when possible.

A query such as:
MATCH (n) RETURN (n)-->()

Would be solved with a plan such as

+Rollup (creates the collection with all the produced paths from RHS)
| \
| +(RHS) Projection (of path)
| |
| +Expand( (n)-->() )
| |
| +Argument
|
+(LHS) AllNodesScan(n)
*/
case class PatternExpressionSolver(pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression) {

  import PatternExpressionSolver.{solvePatternComprehensions, solvePatternExpressions}

  def apply(source: LogicalPlan, expressions: Seq[Expression], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): (LogicalPlan, Seq[Expression]) = {
    val expressionBuild = mutable.ListBuffer[Expression]()
    val patternExpressionSolver = solvePatternExpressions(source.availableSymbols, context, solveds, cardinalities, pathStepBuilder)
    val patternComprehensionSolver = solvePatternComprehensions(source.availableSymbols, context, solveds, cardinalities, pathStepBuilder)

    val finalPlan = expressions.foldLeft(source) {
      case (planAcc, expression: PatternExpression) =>
        val (newPlan, newExpression) = patternExpressionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        expressionBuild += newExpression
        newPlan

      case (planAcc, expression: PatternComprehension) =>
        val (newPlan, newExpression) = patternComprehensionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        expressionBuild += newExpression
        solveds.copy(source.id, newPlan.id)
        newPlan

      case (planAcc, inExpression) =>
        val expression = solveUsingGetDegree(inExpression)
        val (firstStepPlan, firstStepExpression) = patternExpressionSolver.rewriteInnerExpressions(planAcc, expression, context)
        val (newPlan, newExpression) = patternComprehensionSolver.rewriteInnerExpressions(firstStepPlan, firstStepExpression, context)
        expressionBuild += newExpression
        newPlan
    }

    (finalPlan, expressionBuild)
  }

  def apply(source: LogicalPlan, projectionsMap: Map[String, Expression], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): (LogicalPlan, Map[String, Expression]) = {
    val newProjections = Map.newBuilder[String, Expression]
    val patternExpressionSolver = solvePatternExpressions(source.availableSymbols, context, solveds, cardinalities, pathStepBuilder)
    val patternComprehensionSolver = solvePatternComprehensions(source.availableSymbols, context, solveds, cardinalities, pathStepBuilder)

    val plan = projectionsMap.foldLeft(source) {

      // RETURN (a)-->() as X - The top-level expression is a pattern expression
      case (planAcc, (key, expression: PatternExpression)) =>
        val (newPlan, newExpression) = patternExpressionSolver.solveUsingRollUpApply(planAcc, expression, Some(key), context)
        newProjections += (key -> newExpression)
        newPlan

      case (planAcc, (key, expression: PatternComprehension)) =>
        val (newPlan, newExpression) = patternComprehensionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        newProjections += (key -> newExpression)
        newPlan


      // Any other expression, that might contain an inner PatternExpression
      case (planAcc, (key, inExpression)) =>
        val expression = solveUsingGetDegree(inExpression)
        val (firstStepPlan, firstStepExpression) = patternExpressionSolver.rewriteInnerExpressions(planAcc, expression, context)
        val (newPlan, newExpression) = patternComprehensionSolver.rewriteInnerExpressions(firstStepPlan, firstStepExpression, context)

        newProjections += (key -> newExpression)
        newPlan
    }

    (plan, newProjections.result())
  }

  private def solveUsingGetDegree(exp: Expression): Expression =
    exp.endoRewrite(getDegreeRewriter)
}

object PatternExpressionSolver {
  def solvePatternExpressions(availableSymbols: Set[String], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities,
                              pathStepBuilder: EveryPath => PathStep): ListSubQueryExpressionSolver[PatternExpression] = {

    def extractQG(source: LogicalPlan, namedExpr: PatternExpression): QueryGraph = {
      import org.neo4j.cypher.internal.ir.v3_5.helpers.ExpressionConverters._

      val dependencies = namedExpr.
        dependencies.
        map(_.name).
        filter(id => UnNamedNameGenerator.isNamed(id))

      val qgArguments = source.availableSymbols intersect dependencies
      namedExpr.asQueryGraph.withArgumentIds(qgArguments)
    }

    def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
      val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
      val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
      context.forExpressionPlanning(namedNodes, namedRels)
    }

    def createPathExpression(pattern: PatternExpression): PathExpression = {
      val pos = pattern.position
      val path = EveryPath(pattern.pattern.element)
      val step: PathStep = pathStepBuilder(path)
      PathExpression(step)(pos)
    }

    ListSubQueryExpressionSolver[PatternExpression](
      namer = PatternExpressionPatternElementNamer.apply,
      extractQG = extractQG,
      createPlannerContext = createPlannerContext,
      projectionCreator = createPathExpression,
      solveds = solveds,
      cardinalities = cardinalities,
      lastDitch = patternExpressionRewriter(availableSymbols, context, solveds, cardinalities))
  }

  def solvePatternComprehensions(availableSymbols: Set[String], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities,
                                 pathStepBuilder: EveryPath => PathStep): ListSubQueryExpressionSolver[PatternComprehension] = {
    def extractQG(source: LogicalPlan, namedExpr: PatternComprehension) = {
      import org.neo4j.cypher.internal.ir.v3_5.helpers.ExpressionConverters._

      val queryGraph = namedExpr.asQueryGraph
      val args = queryGraph.idsWithoutOptionalMatchesOrUpdates intersect availableSymbols
      queryGraph.withArgumentIds(args)
    }

    def createProjectionToCollect(pattern: PatternComprehension): Expression = pattern.projection

    def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
      val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
      val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
      context.forExpressionPlanning(namedNodes, namedRels)
    }

    ListSubQueryExpressionSolver[PatternComprehension](
      namer = PatternExpressionPatternElementNamer.apply,
      extractQG = extractQG,
      createPlannerContext = createPlannerContext,
      projectionCreator = createProjectionToCollect,
      solveds = solveds,
      cardinalities = cardinalities,
      lastDitch = patternExpressionRewriter(availableSymbols, context, solveds, cardinalities))
  }
}

case class ListSubQueryExpressionSolver[T <: Expression](
    namer: T => (T, Map[PatternElement, Variable]),
    extractQG: (LogicalPlan, T) => QueryGraph,
    createPlannerContext: (LogicalPlanningContext, Map[PatternElement, Variable]) => LogicalPlanningContext,
    projectionCreator: T => Expression,
    lastDitch: Rewriter,
    solveds: Solveds,
    cardinalities: Cardinalities,
    pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression)(implicit m: ClassTag[T]) {

  def solveUsingRollUpApply(source: LogicalPlan, expr: T, maybeKey: Option[String], context: LogicalPlanningContext): (LogicalPlan, Expression) = {

    val key = maybeKey.getOrElse(FreshIdNameGenerator.name(expr.position.bumped()))
    val subQueryPlan = planSubQuery(source, expr, context, solveds, cardinalities)
    val producedPlan = context.logicalPlanProducer.planRollup(source, subQueryPlan.innerPlan, key,
      subQueryPlan.variableToCollect, subQueryPlan.nullableIdentifiers, context)

    (producedPlan, Variable(key)(expr.position))
  }

  def rewriteInnerExpressions(plan: LogicalPlan, expression: Expression, context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    val patternExpressions: Seq[T] = expression.findByAllClass[T]

    patternExpressions.foldLeft(plan, expression) {
      case ((planAcc, expressionAcc), patternExpression) =>
        val (newPlan, introducedVariable) = solveUsingRollUpApply(planAcc, patternExpression, None, context)

        val rewriter = rewriteButStopAtInnerScopes(patternExpression, introducedVariable)
        val rewrittenExpression = expressionAcc.endoRewrite(rewriter)

        if (rewrittenExpression == expressionAcc)
          (planAcc, expressionAcc.endoRewrite(lastDitch))
        else
          (newPlan, rewrittenExpression)
    }
  }

  case class PlannedSubQuery(columnName: String, innerPlan: LogicalPlan, nullableIdentifiers: Set[String]) {
    def variableToCollect = columnName
  }

  private def planSubQuery(source: LogicalPlan, expr: T, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): PlannedSubQuery = {
    val (namedExpr, namedMap) = namer(expr)

    val qg = extractQG(source, namedExpr)
    val innerContext = createPlannerContext(context, namedMap)

    val innerPlan = innerContext.strategy.plan(qg, innerContext, solveds, cardinalities)
    val collectionName = FreshIdNameGenerator.name(expr.position)
    val projectedPath = projectionCreator(namedExpr)
    val projectedInner = context.logicalPlanProducer.planRegularProjection(innerPlan, Map(collectionName -> projectedPath), Map.empty, innerContext)
    PlannedSubQuery(columnName = collectionName, innerPlan = projectedInner, nullableIdentifiers = qg.argumentIds)
  }

  /*
  It's important to not go use RollUpApply if the expression we are working with is inside a loop, or inside a
  conditional expression. If that is not honored, RollUpApply can either produce the wrong results by not having the
  correct scope (when inside a loop), or it can be executed even when not strictly needed (in a conditional)
   */
  private def rewriteButStopAtInnerScopes(oldExp: Expression, newExp: Expression) = {
    val inner = Rewriter.lift {
      case exp if exp == oldExp =>
        newExp
    }
    topDown(inner, stopper = {
      case _: PatternComprehension => false
      case _: ScopeExpression | _: CaseExpression => true
      case f: FunctionInvocation => f.function == Exists
      case _ => false
    })
  }
}

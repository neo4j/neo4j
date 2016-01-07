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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{FreshIdNameGenerator, UnNamedNameGenerator}
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, PatternExpressionPatternElementNamer, patternExpressionRewriter}
import org.neo4j.cypher.internal.frontend.v3_0.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.ast.functions.Exists
import org.neo4j.cypher.internal.frontend.v3_0.{Rewriter, ast, replace}

import scala.collection.mutable

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

  import PatternExpressionSolver.solvePatternExpressions

  def apply(source: LogicalPlan, expressions: Seq[Expression])
           (implicit context: LogicalPlanningContext): (LogicalPlan, Seq[Expression]) = {
    val expressionBuild = mutable.ListBuffer[Expression]()
    val patternSolver = solvePatternExpressions(source.availableSymbols, context, pathStepBuilder)

    val finalPlan = expressions.foldLeft(source) {
      case (planAcc, expression: PatternExpression) =>
        val (newPlan, newExpression) = patternSolver.solveUsingRollUpApply(planAcc, expression, None)
        expressionBuild += newExpression
        newPlan

      case (planAcc, inExpression) =>
        val expression = solveUsingGetDegree(inExpression)
        val (newPlan, newExpression) = patternSolver.rewriteInnerExpressions(planAcc, expression)
        expressionBuild += newExpression
        newPlan
    }

    (finalPlan, expressionBuild.toSeq)
  }

  def apply(source: LogicalPlan, projectionsMap: Map[String, Expression])
           (implicit context: LogicalPlanningContext): (LogicalPlan, Map[String, Expression]) = {
    val newProjections = Map.newBuilder[String, Expression]
    val patternSolver = solvePatternExpressions(source.availableSymbols, context, pathStepBuilder)

    val plan = projectionsMap.foldLeft(source) {

      // RETURN (a)-->() as X - The top-level expression is a pattern expression
      case (planAcc, (key, expression: PatternExpression)) =>
        val (newPlan, newExpression) = patternSolver.solveUsingRollUpApply(planAcc, expression, Some(key))
        newProjections += (key -> newExpression)
        newPlan

      // Any other expression, that might contain an inner PatternExpression
      case (planAcc, (key, inExpression)) =>
        val expression = solveUsingGetDegree(inExpression)
        val (newPlan, newExpression) = patternSolver.rewriteInnerExpressions(planAcc, expression)
        newProjections += (key -> newExpression)
        newPlan
    }

    (plan, newProjections.result())
  }

  private def solveUsingGetDegree(exp: Expression): Expression = exp.endoRewrite(getDegreeRewriter)

}

object PatternExpressionSolver {
  def solvePatternExpressions(availableSymbols: Set[IdName], context: LogicalPlanningContext, pathStepBuilder: EveryPath => PathStep): CollectionSubQueryExpressionSolver[PatternExpression] = {

    def extractQG(source: LogicalPlan, namedExpr: PatternExpression): QueryGraph = {
      import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery.ExpressionConverters._

      val dependencies = namedExpr.
        dependencies.
        map(IdName.fromVariable).
        filter(id => UnNamedNameGenerator.isNamed(id.name))

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
      val path = ast.EveryPath(pattern.pattern.element)
      val step: PathStep = pathStepBuilder(path)
      ast.PathExpression(step)(pos)
    }

    CollectionSubQueryExpressionSolver[PatternExpression](
      namer = PatternExpressionPatternElementNamer.apply,
      extractQG = extractQG,
      createPlannerContext = createPlannerContext,
      projectionCreator = createPathExpression,
      lastDitch = patternExpressionRewriter(availableSymbols, context))
  }

}

case class CollectionSubQueryExpressionSolver[T <: Expression](namer: T => (T, Map[PatternElement, Variable]),
                                                               extractQG: (LogicalPlan, T) => QueryGraph,
                                                               createPlannerContext: (LogicalPlanningContext, Map[PatternElement, Variable]) => LogicalPlanningContext,
                                                               projectionCreator: T => Expression,
                                                               lastDitch: Rewriter,
                                                               pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression)(implicit m: Manifest[T]) {
  def solveUsingRollUpApply(source: LogicalPlan, expr: T, maybeKey: Option[String])
                           (implicit context: LogicalPlanningContext): (LogicalPlan, Expression) = {

    val key = maybeKey.getOrElse(FreshIdNameGenerator.name(expr.position.bumped()))
    val subQueryPlan = planSubQuery(source, expr)
    val producedPlan = context.logicalPlanProducer.planRollup(source, subQueryPlan.innerPlan, IdName(key),
      subQueryPlan.variableToCollect, subQueryPlan.nullableIdentifiers)

    (producedPlan, Variable(key)(expr.position))
  }

  def rewriteInnerExpressions(plan: LogicalPlan, expression: Expression)
                             (implicit context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    val patternExpressions: Seq[T] = expression.findByAllClass[T]

    patternExpressions.foldLeft(plan, expression) {
      case ((planAcc, expressionAcc), patternExpression) =>
        val (newPlan, introducedVariable) = solveUsingRollUpApply(planAcc, patternExpression, None)

        val rewriter = rewriteButStopAtInnerScopes(patternExpression, introducedVariable)
        val rewrittenExpression = expressionAcc.endoRewrite(rewriter)

        assert(rewrittenExpression != expressionAcc)

        (newPlan, rewrittenExpression)
    }
  }

  case class PlannedSubQuery(columnName: String, innerPlan: LogicalPlan, nullableIdentifiers: Set[IdName]) {
    def variableToCollect = IdName(columnName)
  }

  private def planSubQuery(source: LogicalPlan, expr: T)
                          (implicit context: LogicalPlanningContext): PlannedSubQuery = {
    val (namedExpr, namedMap) = namer(expr)

    val qg = extractQG(source, namedExpr)

    val argLeafPlan = Some(context.logicalPlanProducer.planQueryArgumentRow(qg))
    val innerContext = createPlannerContext(context, namedMap)

    val innerPlan = innerContext.strategy.plan(qg)(innerContext, argLeafPlan)
    val collectionName = FreshIdNameGenerator.name(expr.position)
    val projectedPath = projectionCreator(namedExpr)
    val projectedInner = context.logicalPlanProducer.planRegularProjection(innerPlan, Map(collectionName -> projectedPath), Map.empty)(innerContext)
    PlannedSubQuery(columnName = collectionName, innerPlan = projectedInner, nullableIdentifiers = qg.argumentIds)
  }

  private def rewriteButStopAtInnerScopes(oldExp: Expression, newExp: Expression) =
    replace(replacer => {
      // We only unnest the pattern expressions if they are not hiding inside an expression that introduces scope.
      case exp@(_: ScopeExpression) => exp.endoRewrite(lastDitch)
      case exp@FunctionInvocation(FunctionName(name), _, _) if name == Exists.name => exp.endoRewrite(lastDitch)

      case exp if exp == oldExp => newExp
      case exp => replacer.expand(exp)
    })
}


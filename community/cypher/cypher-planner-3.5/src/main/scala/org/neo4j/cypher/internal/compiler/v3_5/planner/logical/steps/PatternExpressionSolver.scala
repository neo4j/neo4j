/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{LogicalPlanningContext, patternExpressionRewriter}
import org.neo4j.cypher.internal.ir.v3_5.{InterestingOrder, QueryGraph}
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.expressions.functions.{Coalesce, Exists, Head}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{PatternExpressionPatternElementNamer, projectNamedPaths}
import org.neo4j.cypher.internal.v3_5.util.{FreshIdNameGenerator, Rewriter, UnNamedNameGenerator, topDown}

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

  def apply(source: LogicalPlan, expressions: Seq[Expression], interestingOrder: InterestingOrder, context: LogicalPlanningContext): (LogicalPlan, Seq[Expression]) = {
    val expressionBuild = mutable.ListBuffer[Expression]()
    val patternExpressionSolver = solvePatternExpressions(source.availableSymbols, interestingOrder, context, pathStepBuilder)
    val patternComprehensionSolver = solvePatternComprehensions(source.availableSymbols, interestingOrder, context, pathStepBuilder)

    val finalPlan = expressions.foldLeft(source) {
      case (planAcc, expression: PatternExpression) =>
        val (newPlan, newExpression) = patternExpressionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        expressionBuild += newExpression
        newPlan

      case (planAcc, expression: PatternComprehension) =>
        val (newPlan, newExpression) = patternComprehensionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        expressionBuild += newExpression
        context.planningAttributes.solveds.copy(source.id, newPlan.id)
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

  def apply(source: LogicalPlan, expression: Expression, interestingOrder: InterestingOrder, context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    val patternExpressionSolver = solvePatternExpressions(source.availableSymbols, interestingOrder, context, pathStepBuilder)
    val patternComprehensionSolver = solvePatternComprehensions(source.availableSymbols, interestingOrder, context, pathStepBuilder)

    expression match {
      case expression: PatternExpression =>
        patternExpressionSolver.solveUsingRollUpApply(source, expression, None, context)

      case expression: PatternComprehension =>
        patternComprehensionSolver.solveUsingRollUpApply(source, expression, None, context)

      case inExpression =>
        val expression = solveUsingGetDegree(inExpression)
        val (firstStepPlan, firstStepExpression) = patternExpressionSolver.rewriteInnerExpressions(source, expression, context)
        patternComprehensionSolver.rewriteInnerExpressions(firstStepPlan, firstStepExpression, context)
    }
  }

  def apply(source: LogicalPlan, projectionsMap: Map[String, Expression], interestingOrder: InterestingOrder, context: LogicalPlanningContext): (LogicalPlan, Map[String, Expression]) = {
    val newProjections = Map.newBuilder[String, Expression]
    val patternExpressionSolver = solvePatternExpressions(source.availableSymbols, interestingOrder, context, pathStepBuilder)
    val patternComprehensionSolver = solvePatternComprehensions(source.availableSymbols, interestingOrder, context, pathStepBuilder)

    val plan = projectionsMap.foldLeft(source) {

      // RETURN (a)-->() as X - The top-level expression is a pattern expression
      case (planAcc, (key, expression: PatternExpression)) =>
        val (newPlan, newExpression) = patternExpressionSolver.solveUsingRollUpApply(planAcc, expression, Some(key), context)
        newProjections += (key -> newExpression)
        newPlan

      // RETURN [(a)-->() | a.foo] - the top-level expression is a pattern comprehension
      case (planAcc, (key, expression: PatternComprehension)) =>
        val (newPlan, newExpression) = patternComprehensionSolver.solveUsingRollUpApply(planAcc, expression, None, context)
        newProjections += (key -> newExpression)
        newPlan


      // Any other expression, that might contain an inner PatternExpression
      case (planAcc, (key, inExpression)) =>
        val expression = solveUsingGetDegree(inExpression)
        val (firstStepPlan, firstStepExpression) = patternComprehensionSolver.rewriteInnerExpressions(planAcc, expression, context)
        val (newPlan, newExpression) = patternExpressionSolver.rewriteInnerExpressions(firstStepPlan, firstStepExpression, context)

        newProjections += (key -> newExpression)
        newPlan
    }

    (plan, newProjections.result())
  }

  private def solveUsingGetDegree(exp: Expression): Expression =
    exp.endoRewrite(getDegreeRewriter)
}

object PatternExpressionSolver {
  def solvePatternExpressions(availableSymbols: Set[String], interestingOrder: InterestingOrder, context: LogicalPlanningContext, pathStepBuilder: EveryPath => PathStep): ListSubQueryExpressionSolver[PatternExpression] = {

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
      interestingOrder = interestingOrder,
      lastDitch = patternExpressionRewriter(availableSymbols, interestingOrder, context))
  }

  def solvePatternComprehensions(availableSymbols: Set[String], interestingOrder: InterestingOrder, context: LogicalPlanningContext, pathStepBuilder: EveryPath => PathStep): ListSubQueryExpressionSolver[PatternComprehension] = {
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
      interestingOrder = interestingOrder,
      lastDitch = patternExpressionRewriter(availableSymbols, interestingOrder, context))
  }
}

case class ListSubQueryExpressionSolver[T <: Expression](namer: T => (T, Map[PatternElement, Variable]),
                                                         extractQG: (LogicalPlan, T) => QueryGraph,
                                                         createPlannerContext: (LogicalPlanningContext, Map[PatternElement, Variable]) => LogicalPlanningContext,
                                                         projectionCreator: T => Expression,
                                                         lastDitch: Rewriter,
                                                         interestingOrder: InterestingOrder,
                                                         pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression)(implicit m: ClassTag[T]) {

  def solveUsingRollUpApply(source: LogicalPlan, expr: T, maybeKey: Option[String], context: LogicalPlanningContext): (LogicalPlan, Expression) = {

    val key = maybeKey.getOrElse(FreshIdNameGenerator.name(expr.position.bumped()))
    val subQueryPlan = planSubQuery(source, expr, context)
    val producedPlan = context.logicalPlanProducer.planRollup(source, subQueryPlan.innerPlan, key,
      subQueryPlan.variableToCollect, subQueryPlan.nullableIdentifiers, context)

    (producedPlan, Variable(key)(expr.position))
  }

  def rewriteInnerExpressions(plan: LogicalPlan, expression: Expression, context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    val patternExpressions: Seq[T] = expression.findByAllClass[T]

    patternExpressions.foldLeft(plan, expression) {
      case ((planAcc, expressionAcc), patternExpression) =>
        val (newPlan, introducedVariable) = solveUsingRollUpApply(planAcc, patternExpression, None, context)

        val rewriter = rewriteButStopIfRollUpApplyForbidden(patternExpression, introducedVariable)
        val rewrittenExpression = expressionAcc.endoRewrite(rewriter)

        if (rewrittenExpression == expressionAcc)
          (planAcc, expressionAcc.endoRewrite(lastDitch))
        else
          (newPlan, rewrittenExpression)
    }
  }

  case class PlannedSubQuery(columnName: String, innerPlan: LogicalPlan, nullableIdentifiers: Set[String]) {
    def variableToCollect: String = columnName
  }

  private def planSubQuery(source: LogicalPlan, expr: T, context: LogicalPlanningContext) = {
    val (namedExpr, namedMap) = namer(expr)

    val qg = extractQG(source, namedExpr)
    val innerContext = createPlannerContext(context, namedMap)

    val innerPlan = innerContext.strategy.plan(qg, interestingOrder, innerContext)
    val collectionName = FreshIdNameGenerator.name(expr.position)
    val projectedPath = projectionCreator(namedExpr)
    val projectedInner = projection(innerPlan, Map(collectionName -> projectedPath), Map(collectionName -> projectedPath), interestingOrder, innerContext)
    val nullableIdentifiers = (qg.patternNodes ++ qg.patternRelationships.map(_.name)).filter(source.availableSymbols)
    PlannedSubQuery(columnName = collectionName, innerPlan = projectedInner, nullableIdentifiers = nullableIdentifiers)
  }

  /*
   * It's important to not go use RollUpApply if the expression we are working with is:
   *
   * a) inside a loop. If that is not honored, it will produce the wrong results by not having the correct scope.
   * b) inside a conditional expression. Otherwise it can be executed even when not strictly needed.
   * c) inside an expression that accessed only part of the list. Otherwise we do too much work. To avoid that we inject a Limit into the
   *    NestedPlanExpression.
   *
   */
  private def rewriteButStopIfRollUpApplyForbidden(oldExp: Expression, newExp: Expression): Rewriter = {
    val inner = Rewriter.lift {
      case exp if exp == oldExp =>
        newExp
    }
    topDown(inner, stopper = {
      case _: PatternComprehension => false
      // Loops
      case _: ScopeExpression => true
      // Conditionals & List accesses
      case _: CaseExpression => true
      case _: ContainerIndex => true
      case _: ListSlice => true
      case f: FunctionInvocation => f.function == Exists || f.function == Coalesce || f.function == Head
      case _ => false
    })
  }
}

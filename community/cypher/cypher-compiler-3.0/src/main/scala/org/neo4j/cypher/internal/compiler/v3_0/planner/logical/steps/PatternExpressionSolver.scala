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

import org.neo4j.cypher.internal.compiler.v3_0.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{FreshIdNameGenerator, UnNamedNameGenerator}
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, PatternExpressionPatternElementNamer, patternExpressionRewriter}
import org.neo4j.cypher.internal.frontend.v3_0.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.ast.functions.Exists
import org.neo4j.cypher.internal.frontend.v3_0.{ExpressionWithInnerScope, SemanticDirection, ast, replace}

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

  def apply(source: LogicalPlan, expressions: Seq[Expression])
           (implicit context: LogicalPlanningContext): (LogicalPlan, Seq[Expression]) = {
    val lastDitch = patternExpressionRewriter(source.availableSymbols, context)
    val expressionBuild = mutable.ListBuffer[Expression]()
    val finalPlan = expressions.foldLeft(source) {
      case (planAcc, expression: PatternExpression) =>
        val (newPlan, newExpression) = solveUsingRollUpApply(planAcc, expression, None)
        expressionBuild += newExpression
        newPlan

      case (planAcc, inExpression) =>
        val (newPlan, newExpression) = rewriteInnerExpressions(planAcc, inExpression, lastDitch)
        expressionBuild += newExpression
        newPlan
    }

    (finalPlan, expressionBuild.toSeq)
  }

  def apply(source: LogicalPlan, projectionsMap: Map[String, Expression])
           (implicit context: LogicalPlanningContext): (LogicalPlan, Map[String, Expression]) = {
    val newProjections = Map.newBuilder[String, Expression]
    val lastDitch = patternExpressionRewriter(source.availableSymbols, context)
    val plan = projectionsMap.foldLeft(source) {

      // RETURN (a)-->() as X - The top-level expression is a pattern expression
      case (plan, (key, expression: PatternExpression)) =>
        val (newPlan, newExpression) = solveUsingRollUpApply(plan, expression, Some(key))
        newProjections += (key -> newExpression)
        newPlan

      // Any other expression, that might contain an inner PatternExpression
      case (plan, (key, inExpression)) =>
        val (newPlan, newExpression) = rewriteInnerExpressions(plan, inExpression, lastDitch)
        newProjections += (key -> newExpression)
        newPlan
    }

    (plan, newProjections.result())
  }

  private def rewriteInnerExpressions(plan: LogicalPlan, inExpression: Expression, lastDitch: patternExpressionRewriter)
                                     (implicit context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    val expression = solveUsingGetDegree(inExpression)

    val patternExpressions: Seq[PatternExpression] = expression.findByAllClass[PatternExpression]

    patternExpressions.foldLeft(plan, expression) {
      case ((planAcc, expressionAcc), patternExpression) =>
        val (newPlan, introducedVariable) = solveUsingRollUpApply(planAcc, patternExpression, None)

        val rewriter = rewriteButStopAtInnerScopes(patternExpression, introducedVariable, lastDitch)
        val rewrittenExpression = expressionAcc.endoRewrite(rewriter)

        // If for some reason nothing was changed, make sure we also return the original plan
        if (rewrittenExpression == expressionAcc)
          (planAcc, expressionAcc)
        else
          (newPlan, rewrittenExpression)
    }
  }

  private def rewriteButStopAtInnerScopes(oldExp: Expression, newExp: Expression, lastDitch: patternExpressionRewriter) =
    replace(replacer => {
      // We only unnest the pattern expressions if they are not hiding inside an expression that introduces scope.
      case exp@(_: ExpressionWithInnerScope) => exp.endoRewrite(lastDitch)
      case exp@FunctionInvocation(FunctionName(name), _, _) if name == Exists.name => exp.endoRewrite(lastDitch)


      case exp if exp == oldExp => newExp
      case exp => replacer.expand(exp)
    })

  private def solveUsingGetDegree(exp: Expression): Expression = exp.endoRewrite(getDegreeRewriter)

  private val getDegreeRewriter = replace(replacer => {
    // Top-Down:
    // Do not traverse into NestedPlanExpressions as they have been optimized already by an earlier call to plan
    case that: NestedPlanExpression =>
      replacer.stop(that)

    case that =>
      // Bottom-up:
      // Replace function invocations with more efficient expressions
      replacer.expand(that) match {

        // LENGTH( (a)-[]->() )
        case func@FunctionInvocation(_, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(node), List(), None), RelationshipPattern(None, _, types, None, None, dir), NodePattern(None, List(), None))))))
          if func.function.contains(functions.Length) || func.function.contains(functions.Size) =>
          calculateUsingGetDegree(func, node, types, dir)

        // LENGTH( ()-[]->(a) )
        case func@FunctionInvocation(_, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, List(), None), RelationshipPattern(None, _, types, None, None, dir), NodePattern(Some(node), List(), None))))))
          if func.function.contains(functions.Length) || func.function.contains(functions.Size) =>
          calculateUsingGetDegree(func, node, types, dir.reversed)

        case rewritten =>
          rewritten
      }
  })

  private def calculateUsingGetDegree(func: FunctionInvocation, node: Variable, types: Seq[RelTypeName], dir: SemanticDirection): Expression = {
    types
      .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(func.position))
      .getOrElse(GetDegree(node, None, dir)(func.position))
  }

  private def solveUsingRollUpApply(source: LogicalPlan, expr: PatternExpression, maybeKey: Option[String])
                                   (implicit context: LogicalPlanningContext): (LogicalPlan, Expression) = {

    val (namedExpr, namedMap) = PatternExpressionPatternElementNamer(expr)

    val qg = extractQG(source, namedExpr)

    val argLeafPlan = Some(context.logicalPlanProducer.planQueryArgumentRow(qg))
    val patternPlanningContext = createPlannerContext(context, namedMap)

    val innerPlan = patternPlanningContext.strategy.plan(qg)(patternPlanningContext, argLeafPlan)
    val collectionName = FreshIdNameGenerator.name(expr.position)
    val projectedPath = createPathExpression(namedExpr)
    val projectedInner = context.logicalPlanProducer.planRegularProjection(innerPlan, Map(collectionName -> projectedPath), Map.empty)(patternPlanningContext)
    val key = maybeKey.getOrElse(FreshIdNameGenerator.name(expr.position.bumped()))
    val producedPlan = context.logicalPlanProducer.planRollup(source, projectedInner, IdName(key), IdName(collectionName), qg.argumentIds)

    (producedPlan, Variable(key)(expr.position))
  }

  private def createPathExpression(pattern: PatternExpression): PathExpression = {
    val pos = pattern.position
    val path = ast.EveryPath(pattern.pattern.element)
    val step: PathStep = pathStepBuilder(path)
    ast.PathExpression(step)(pos)
  }

  private def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
    val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
    context.forExpressionPlanning(namedNodes, namedRels)
  }

  private def extractQG(source: LogicalPlan, namedExpr: PatternExpression): QueryGraph = {
    import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery.ExpressionConverters._

    val dependencies = namedExpr.
      dependencies.
      map(IdName.fromVariable).
      filter(id => UnNamedNameGenerator.isNamed(id.name))

    val qgArguments = source.availableSymbols intersect dependencies
    namedExpr.asQueryGraph.withArgumentIds(qgArguments)
  }
}

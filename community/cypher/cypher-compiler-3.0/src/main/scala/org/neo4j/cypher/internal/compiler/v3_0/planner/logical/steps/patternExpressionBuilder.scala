/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{UnNamedNameGenerator, FreshIdNameGenerator}
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, PatternExpressionPatternElementNamer}
import org.neo4j.cypher.internal.frontend.v3_0.ast
import org.neo4j.cypher.internal.frontend.v3_0.ast._

/*
Prepares expressions containing pattern expressions by solving them in a sub-query through RollUp,
and replacing the original expression with an identifier

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
case class patternExpressionBuilder(pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression) {
  def apply(source: LogicalPlan, projectionsMap: Map[String, Expression])
           (implicit context: LogicalPlanningContext): (LogicalPlan, Map[String, Expression]) = {

    projectionsMap.foldLeft(source, Map.empty[String, Expression]) {
      case ((plan, projections), (key, expression)) =>
        val (newPlan, newExpression) = fixUp(plan, expression, maybeKey = Some(key))

        (newPlan, projections + (key -> newExpression))
    }
  }

  private def fixUp(source: LogicalPlan, expression: Expression, maybeKey: Option[String])
                   (implicit context: LogicalPlanningContext): (LogicalPlan, Expression) = {
    expression match {
      case expr: PatternExpression =>
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

      case e =>
        (source, expression)
    }
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

  //  def apply(source: LogicalPlan, expressions: Seq[Expression])
  //           (implicit context: LogicalPlanningContext): (LogicalPlan, Seq[Expression]) = {
  //    expressions.foldLeft(source, Seq.empty[Expression]) {
  //      case ((plan, apa), expression) =>
  //
  //        val x = expression.findByAllClass[PatternExpression]
  //
  //        val (newPlan, replacements) = x.foldLeft(source, Map.empty[PatternExpression, Expression]) {
  //          case ((plan, replacementsAcc), patternExpression) =>
  //            (plan, replacementsAcc + (patternExpression -> patternExpression))
  //        }
  //
  ////        val (newPlan, newExpression) = fixUp(plan, expression, maybeKey = None)
  //
  //        (newPlan, apa :+ expression)
  //    }
  //  }

}

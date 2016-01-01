/**
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.OtherConverters._
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters.PatternPartToPathExpression
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.{AggregationExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_1.commands.{True, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class PipeExecutionBuilderContext(patternExpressionPlans: Map[ast.PatternExpression, LogicalPlan]) {
  def plan(expr: ast.PatternExpression) = patternExpressionPlans(expr)
}

class PipeExecutionPlanBuilder(monitors: Monitors) {

  def build(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext): PipeInfo = {
    val updating = false

    object buildPipeExpressions extends Rewriter {
      val instance = Rewriter.lift {
        case pattern: ast.PatternExpression =>
          val pos = pattern.position
          val pipe = buildPipe(context.plan(pattern))
          val step = PatternPartToPathExpression.patternPartPathExpression(ast.EveryPath(pattern.pattern.element))
          ast.NestedPipeExpression(pipe, ast.PathExpression(step)(pos))(pos)
      }

      def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)
    }

    def buildExpression(expr: ast.Expression): CommandExpression = {
      val rewrittenExpr = expr.endoRewrite(buildPipeExpressions)
      rewrittenExpr.asCommandExpression
    }

    def buildPredicate(expr: ast.Expression): CommandPredicate = {
      val rewrittenExpr = expr.endoRewrite(buildPipeExpressions)
      rewrittenExpr.asCommandPredicate
    }

    def buildPipe(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext): Pipe = {
      implicit val monitor = monitors.newMonitor[PipeMonitor]()
      plan match {
        case Projection(left, expressions) =>
          ProjectionNewPipe(buildPipe(left), expressions.mapValues(buildExpression))

        case sr @ SingleRow(ids) =>
          NullPipe(new SymbolTable(sr.typeInfo))

        case AllNodesScan(IdName(id)) =>
          AllNodesScanPipe(id)

        case NodeByLabelScan(IdName(id), label) =>
          NodeByLabelScanPipe(id, label)

        case NodeByIdSeek(IdName(id), nodeIdExpr) =>
          NodeByIdSeekPipe(id, nodeIdExpr.map(buildExpression))

        case DirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode)) =>
          DirectedRelationshipByIdSeekPipe(id, relIdExpr.map(buildExpression), toNode, fromNode)

        case UndirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode)) =>
          UndirectedRelationshipByIdSeekPipe(id, relIdExpr.map(buildExpression), toNode, fromNode)

        case NodeIndexSeek(IdName(id), label, propertyKey, valueExpr) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = false)

        case NodeIndexUniqueSeek(IdName(id), label, propertyKey, valueExpr) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = true)

        case Selection(predicates, left) =>
          FilterPipe(buildPipe(left), predicates.map(buildPredicate).reduce(_ ++ _))

        case CartesianProduct(left, right) =>
          CartesianProductPipe(buildPipe(left), buildPipe(right))

        case Expand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), SimplePatternLength) =>
          ExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name))

        case Expand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), VarPatternLength(min, max)) =>
          VarLengthExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name), min, max)

        case OptionalExpand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), SimplePatternLength, predicates) =>
          val predicate = predicates.map(buildPredicate).reduceOption(_ ++ _).getOrElse(True())
          OptionalExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name), predicate)

        case NodeHashJoin(node, left, right) =>
          NodeHashJoinPipe(node.name, buildPipe(left), buildPipe(right))

        case OuterHashJoin(node, left, right) =>
          NodeOuterHashJoinPipe(node.name, buildPipe(left), buildPipe(right), (right.availableSymbols -- left.availableSymbols).map(_.name))

        case Optional(inner) =>
          OptionalPipe(inner.availableSymbols.map(_.name), buildPipe(inner))

        case Apply(outer, inner) =>
          ApplyPipe(buildPipe(outer), buildPipe(inner))

        case SemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = false)

        case AntiSemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = true)

        case LetSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = false)

        case LetAntiSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = true)

        case apply@SelectOrSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), buildPredicate(predicate), negated = false)

        case apply@SelectOrAntiSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), buildPredicate(predicate), negated = true)

        case apply@LetSelectOrSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, buildPredicate(predicate), negated = false)

        case apply@LetSelectOrAntiSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, buildPredicate(predicate), negated = true)

        case Sort(left, sortItems) =>
          SortPipe(buildPipe(left), sortItems)

        case Skip(input, count) =>
          SkipPipe(buildPipe(input), buildExpression(count))

        case Limit(input, count) =>
          LimitPipe(buildPipe(input), buildExpression(count))

        case SortedLimit(input, exp, sortItems) =>
          TopPipe(buildPipe(input), sortItems.map(_.asCommandSortItem).toList, exp.asCommandExpression)

        case Aggregation(input, groupingExpressions, aggregatingExpressions) =>
          EagerAggregationPipe(
            buildPipe(input),
            groupingExpressions.mapValues(_.asCommandExpression),
            aggregatingExpressions.mapValues(_.asCommandExpression.asInstanceOf[AggregationExpression]))

        case FindShortestPaths(input, shortestPath) =>
          val legacyShortestPaths = shortestPath.expr.asLegacyPatterns(shortestPath.name.map(_.name))
          val legacyShortestPath = legacyShortestPaths.head
          new ShortestPathPipe(buildPipe(input), legacyShortestPath)

        case Union(lhs, rhs) =>
          NewUnionPipe(buildPipe(lhs), buildPipe(rhs))

        case UnwindPlan(lhs, identifier, collection) =>
          UnwindPipe(buildPipe(lhs), collection.asCommandExpression, identifier.name)

        case _ =>
          throw new CantHandleQueryException
      }
    }

    val topLevelPipe = buildPipe(plan)

    PipeInfo(topLevelPipe, updating, None)
  }
}

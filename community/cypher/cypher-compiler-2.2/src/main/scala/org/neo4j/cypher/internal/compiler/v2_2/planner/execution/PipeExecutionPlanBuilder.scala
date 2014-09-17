/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.OtherConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.{ExpressionConverters, OtherConverters, PatternConverters}
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{AggregationExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{True, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable
import org.neo4j.graphdb.Relationship
import org.neo4j.cypher.internal.helpers.Eagerly

case class PipeExecutionBuilderContext(f: ast.PatternExpression => LogicalPlan) {
  def plan(expr: ast.PatternExpression) = f(expr)
}

class PipeExecutionPlanBuilder(monitors: Monitors) {

  def build(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext): PipeInfo = {
    val updating = false

    object buildPipeExpressions extends Rewriter {
      val instance = Rewriter.lift {
        case pattern: ast.PatternExpression =>
          val pos = pattern.position
          val pipe = buildPipe(context.plan(pattern))
          val step = projectNamedPaths.patternPartPathExpression(ast.EveryPath(pattern.pattern.element))
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
          ProjectionNewPipe(buildPipe(left), Eagerly.immutableMapValues(expressions, buildExpression))

        case ProjectEndpoints(left, rel, start, end, directed, length) =>
          ProjectEndpointsPipe(buildPipe(left), rel.name, start.name, end.name, directed, length.isSimple)

        case sr @ SingleRow(ids) =>
          NullPipe(new SymbolTable(sr.typeInfo))

        case AllNodesScan(IdName(id), _) =>
          AllNodesScanPipe(id)

        case NodeByLabelScan(IdName(id), label, _) =>
          NodeByLabelScanPipe(id, label)

        case NodeByIdSeek(IdName(id), nodeIdExpr, _) =>
          NodeByIdSeekPipe(id, nodeIdExpr.asEntityByIdRhs)

        case DirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          DirectedRelationshipByIdSeekPipe(id, relIdExpr.asEntityByIdRhs, toNode, fromNode)

        case UndirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          UndirectedRelationshipByIdSeekPipe(id, relIdExpr.asEntityByIdRhs, toNode, fromNode)

        case NodeIndexSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = false)

        case NodeIndexUniqueSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = true)

        case Selection(predicates, left) =>
          FilterPipe(buildPipe(left), predicates.map(buildPredicate).reduce(_ ++ _))

        case CartesianProduct(left, right) =>
          CartesianProductPipe(buildPipe(left), buildPipe(right))

        case Expand(left, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName), SimplePatternLength, _) =>
          ExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name))

        case Expand(left, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName), VarPatternLength(min, max), predicates) =>
          val (keys, exprs) = predicates.unzip
          val commands = exprs.map(buildPredicate)
          val predicate = (context: ExecutionContext, state: QueryState, rel: Relationship) => {
            keys.zip(commands).forall { case (identifier: Identifier, expr: CommandPredicate) =>
              context(identifier.name) = rel
              val result = expr.isTrue(context)(state)
              context.remove(identifier.name)
              result
            }
          }
          VarLengthExpandPipe(buildPipe(left), fromName, relName, toName, dir, projectedDir, types.map(_.name), min, max, predicate)

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
            Eagerly.immutableMapValues[String, ast.Expression, commands.expressions.Expression](groupingExpressions, x => x.asCommandExpression),
            Eagerly.immutableMapValues[String, ast.Expression, AggregationExpression](aggregatingExpressions, x => x.asCommandExpression.asInstanceOf[AggregationExpression])
          )

        case FindShortestPaths(input, shortestPath) =>
          val legacyShortestPaths = shortestPath.expr.asLegacyPatterns(shortestPath.name.map(_.name))
          val legacyShortestPath = legacyShortestPaths.head
          new ShortestPathPipe(buildPipe(input), legacyShortestPath)

        case Union(lhs, rhs) =>
          NewUnionPipe(buildPipe(lhs), buildPipe(rhs))

        case UnwindCollection(lhs, identifier, collection) =>
          UnwindPipe(buildPipe(lhs), collection.asCommandExpression, identifier.name)

        case x =>
          throw new CantHandleQueryException(x.toString)
      }
    }

    val topLevelPipe = buildPipe(plan)

    PipeInfo(topLevelPipe, updating, None, CypherVersion.v2_2_cost)
  }
}

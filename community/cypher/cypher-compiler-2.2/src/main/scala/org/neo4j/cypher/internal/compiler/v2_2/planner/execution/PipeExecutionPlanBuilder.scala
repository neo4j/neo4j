/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.OtherConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.StatementConverters
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Expression, Identifier, NodeStartItem, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{AggregationExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{EntityProducerFactory, Predicate => CommandPredicate, True}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PipeInfo, PlanFingerprint}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{LazyTypes, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{CantHandleQueryException, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable
import org.neo4j.cypher.internal.helpers.Eagerly
import org.neo4j.graphdb.Relationship
import org.neo4j.helpers.Clock

case class PipeExecutionBuilderContext(cardinality: Metrics.CardinalityModel, semanticTable: SemanticTable, plannerName: PlannerName)

class PipeExecutionPlanBuilder(clock: Clock, monitors: Monitors) {

  val entityProducerFactory = new EntityProducerFactory
  val resolver = new KeyTokenResolver

  def build(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeInfo = {
    implicit val table: SemanticTable = context.semanticTable
    val updating = false

    def buildPipe(plan: LogicalPlan): Pipe = {
      implicit val monitor = monitors.newMonitor[PipeMonitor]()

      val result: Pipe with RonjaPipe = plan match {
        case Projection(left, expressions) =>
          ProjectionNewPipe(buildPipe(left), Eagerly.immutableMapValues(expressions, buildExpression))()

        case ProjectEndpoints(left, rel, start, startInScope, end, endInScope, types, directed, length) =>
          ProjectEndpointsPipe(buildPipe(left), rel.name,
            start.name, startInScope,
            end.name, endInScope,
            types.map(LazyTypes.apply), directed, length.isSimple)()

        case sr @ SingleRow() =>
          SingleRowPipe()

        case sr @ Argument(ids) =>
          ArgumentPipe(new SymbolTable(sr.typeInfo))()

        case AllNodesScan(IdName(id), _) =>
          AllNodesScanPipe(id)()

        case NodeByLabelScan(IdName(id), label, _) =>
          NodeByLabelScanPipe(id, label)()

        case NodeByIdSeek(IdName(id), nodeIdExpr, _) =>
          NodeByIdSeekPipe(id, nodeIdExpr.asCommandEntityByIdRhs)()

        case DirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          DirectedRelationshipByIdSeekPipe(id, relIdExpr.asCommandEntityByIdRhs, toNode, fromNode)()

        case UndirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          UndirectedRelationshipByIdSeekPipe(id, relIdExpr.asCommandEntityByIdRhs, toNode, fromNode)()

        case NodeIndexSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = false)()

        case NodeIndexUniqueSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = true)()

        case Selection(predicates, left) =>
          FilterPipe(buildPipe(left), predicates.map(buildPredicate).reduce(_ ++ _))()

        case CartesianProduct(left, right) =>
          CartesianProductPipe(buildPipe(left), buildPipe(right))()

        case Expand(left, IdName(fromName), dir, types: Seq[RelTypeName], IdName(toName), IdName(relName), ExpandAll) =>
          ExpandAllPipe(buildPipe(left), fromName, relName, toName, dir, LazyTypes(types))()

        case Expand(left, IdName(fromName), dir, types: Seq[RelTypeName], IdName(toName), IdName(relName), ExpandInto) =>
          ExpandIntoPipe(buildPipe(left), fromName, relName, toName, dir, LazyTypes(types))()

        case OptionalExpand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandAll, predicates) =>
          val predicate = predicates.map(buildPredicate).reduceOption(_ ++ _).getOrElse(True())
          OptionalExpandAllPipe(buildPipe(left), fromName, relName, toName, dir, LazyTypes(types), predicate)()

        case OptionalExpand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandInto, predicates) =>
          val predicate = predicates.map(buildPredicate).reduceOption(_ ++ _).getOrElse(True())
          OptionalExpandIntoPipe(buildPipe(left), fromName, relName, toName, dir, LazyTypes(types), predicate)()

        case VarExpand(left, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName), VarPatternLength(min, max), expansionMode, predicates) =>
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

          val nodeInScope = expansionMode match {
            case ExpandAll => false
            case ExpandInto => true
          }
          VarLengthExpandPipe(buildPipe(left), fromName, relName, toName, dir, projectedDir,
            LazyTypes(types), min, max, nodeInScope, predicate)()

        case NodeHashJoin(nodes, left, right) =>
          NodeHashJoinPipe(nodes.map(_.name), buildPipe(left), buildPipe(right))()

        case OuterHashJoin(nodes, left, right) =>
          NodeOuterHashJoinPipe(nodes.map(_.name), buildPipe(left), buildPipe(right), (right.availableSymbols -- left.availableSymbols).map(_.name))()

        case Optional(inner) =>
          OptionalPipe(inner.availableSymbols.map(_.name), buildPipe(inner))()

        case Apply(outer, inner) =>
          ApplyPipe(buildPipe(outer), buildPipe(inner))()

        case SemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = false)()

        case AntiSemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = true)()

        case LetSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = false)()

        case LetAntiSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = true)()

        case apply@SelectOrSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), buildPredicate(predicate), negated = false)()

        case apply@SelectOrAntiSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), buildPredicate(predicate), negated = true)()

        case apply@LetSelectOrSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, buildPredicate(predicate), negated = false)()

        case apply@LetSelectOrAntiSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, buildPredicate(predicate), negated = true)()

        case Sort(left, sortItems) =>
          SortPipe(buildPipe(left), sortItems)()

        case Skip(lhs, count) =>
          SkipPipe(buildPipe(lhs), buildExpression(count))()

        case Limit(lhs, count) =>
          LimitPipe(buildPipe(lhs), buildExpression(count))()

        case SortedLimit(lhs, exp, sortItems) =>
          TopPipe(buildPipe(lhs), sortItems.map(_.asCommandSortItem).toList, exp.asCommandExpression)()

        // TODO: Maybe we shouldn't encode distinct as an empty aggregation.
        case Aggregation(Projection(source, expressions), groupingExpressions, aggregatingExpressions)
          if aggregatingExpressions.isEmpty && expressions == groupingExpressions =>
          DistinctPipe(buildPipe(source), groupingExpressions.mapValues(_.asCommandExpression))()

        case Aggregation(source, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
          DistinctPipe(buildPipe(source), groupingExpressions.mapValues(_.asCommandExpression))()

        case Aggregation(source, groupingExpressions, aggregatingExpressions) =>
          EagerAggregationPipe(
            buildPipe(source),
            groupingExpressions.keySet,
            Eagerly.immutableMapValues[String, ast.Expression, AggregationExpression](aggregatingExpressions, buildExpression(_).asInstanceOf[AggregationExpression])
          )()

        case FindShortestPaths(source, shortestPath) =>
          val legacyShortestPaths = shortestPath.expr.asLegacyPatterns(shortestPath.name.map(_.name))
          val legacyShortestPath = legacyShortestPaths.head
          new ShortestPathPipe(buildPipe(source), legacyShortestPath)()

        case Union(lhs, rhs) =>
          NewUnionPipe(buildPipe(lhs), buildPipe(rhs))()

        case UnwindCollection(lhs, identifier, collection) =>
          UnwindPipe(buildPipe(lhs), collection.asCommandExpression, identifier.name)()

        case LegacyIndexSeek(id, hint: NodeStartItem, _) =>
          val source = new SingleRowPipe()
          val ep = entityProducerFactory.nodeStartItems((planContext, StatementConverters.StartItemConverter(hint).asCommandStartItem))
          NodeStartPipe(source, id.name, ep)()

        case x =>
          throw new CantHandleQueryException(x.toString)
      }

      result.withEstimatedCardinality(plan.solved.estimatedCardinality.amount)
    }

    object buildPipeExpressions extends Rewriter {
      val instance = Rewriter.lift {
        case ast.NestedPlanExpression(patternPlan, pattern) =>
          val pos = pattern.position
          val pipe = buildPipe(patternPlan)
          val step = projectNamedPaths.patternPartPathExpression(ast.EveryPath(pattern.pattern.element))
          val result = ast.NestedPipeExpression(pipe, ast.PathExpression(step)(pos))(pos)
          result
      }

      def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)
    }

    def buildExpression(expr: ast.Expression): CommandExpression = {
      val rewrittenExpr = expr.endoRewrite(buildPipeExpressions)

      rewrittenExpr.asCommandExpression.rewrite(resolver.resolveExpressions(_, planContext))
    }

    def buildPredicate(expr: ast.Expression): CommandPredicate = {
      val rewrittenExpr: Expression = expr.endoRewrite(buildPipeExpressions)

      rewrittenExpr.asCommandPredicate.rewrite(resolver.resolveExpressions(_, planContext)).asInstanceOf[CommandPredicate]
    }

    val topLevelPipe = buildPipe(plan)

    val fingerprint = planContext.statistics match {
      case igs: InstrumentedGraphStatistics =>
        Some(PlanFingerprint(clock.currentTimeMillis(), planContext.txIdProvider(), igs.snapshot.freeze))
      case _ =>
        None
    }

    PipeInfo(topLevelPipe, updating, None, fingerprint, context.plannerName)
  }
}

/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commandExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{ColumnOrder => _, _}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.{expressions => slottedExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{LongSlot, PipeBuilder, PipeExecutionBuilderContext, PipelineInformation, _}
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticTable, ast => frontEndAst}
import org.neo4j.cypher.internal.ir.v3_3.{IdName, VarPatternLength}
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans._

class SlottedPipeBuilder(fallback: PipeBuilder,
                         expressionConverters: ExpressionConverters,
                         monitors: Monitors,
                         pipelines: Map[LogicalPlanId, PipelineInformation],
                         readOnly: Boolean,
                         rewriteAstExpression: (frontEndAst.Expression) => frontEndAst.Expression)
                        (implicit context: PipeExecutionBuilderContext, planContext: PlanContext)
  extends PipeBuilder {

  private val convertExpressions: (frontEndAst.Expression) => commandExpressions.Expression =
    rewriteAstExpression andThen expressionConverters.toCommandExpression

  override def build(plan: LogicalPlan): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = plan.assignedId
    val pipelineInformation = pipelines(plan.assignedId)

    plan match {
      case AllNodesScan(IdName(column), _) =>
        AllNodesScanSlottedPipe(column, pipelineInformation)(id)

      case NodeIndexScan(IdName(column), label, propertyKeys, _) =>
        NodeIndexScanSlottedPipe(column, label, propertyKeys, pipelineInformation)(id)

      case NodeIndexSeek(IdName(column), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, propertyKeys,
                                  valueExpr.map(convertExpressions), indexSeekMode, pipelineInformation)(id)

      case NodeUniqueIndexSeek(IdName(column), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, propertyKeys,
                                  valueExpr.map(convertExpressions), indexSeekMode, pipelineInformation)(id = id)

      case _: Argument =>
        ArgumentSlottedPipe(pipelineInformation)(id)

      case NodeByLabelScan(IdName(column), label, _) =>
        NodesByLabelScanSlottedPipe(column, LazyLabel(label), pipelineInformation)(id)

      case SingleRow() =>
        SingleRowSlottedPipe(pipelineInformation)(id)

      case _ =>
        throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")

    }
  }

  override def build(plan: LogicalPlan, source: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = plan.assignedId
    val pipeline = pipelines(plan.assignedId)

    plan match {
      case ProduceResult(columns, _) =>
        val runtimeColumns = createProjectionsForResult(columns, pipeline)
        ProduceResultSlottedPipe(source, runtimeColumns)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
        val fromSlot = pipeline.getLongOffsetFor(from)
        val relSlot = pipeline.getLongOffsetFor(relName)
        val toSlot = pipeline.getLongOffsetFor(to)
        ExpandAllSlottedPipe(source, fromSlot, relSlot, toSlot, dir, LazyTypes(types.toArray), pipeline)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandInto) =>
        val fromOffset = pipeline.getLongOffsetFor(from)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(to)
        ExpandIntoSlottedPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types.toArray), pipeline)(id)

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandAll, predicates) =>
        val fromOffset = pipeline.getLongOffsetFor(fromName)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(toName)
        val predicate: Predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandAllSlottedPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types.toArray), predicate,
                                      pipeline)(id)

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandInto, predicates) =>
        val fromOffset = pipeline.getLongOffsetFor(fromName)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(toName)
        val predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandIntoSlottedPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types.toArray), predicate,
                                       pipeline)(id)

      case VarExpand(sourcePlan, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName),
                     VarPatternLength(min, max), expansionMode, IdName(tempNode), IdName(tempEdge), nodePredicate,
                     edgePredicate, _) =>

        val shouldExpandAll = expansionMode match {
          case ExpandAll => true
          case ExpandInto => false
        }
        val fromOffset = pipeline.getLongOffsetFor(fromName)
        val toOffset = pipeline.getLongOffsetFor(toName)
        val relOffset = pipeline.getReferenceOffsetFor(relName)

        // The node/edge predicates are evaluated on the incoming pipeline, not the produced one
        val incomingPipeline = pipelines(sourcePlan.assignedId)
        val tempNodeOffset = incomingPipeline.getLongOffsetFor(tempNode)
        val tempEdgeOffset = incomingPipeline.getLongOffsetFor(tempEdge)
        val sizeOfTemporaryStorage = 2
        VarLengthExpandSlottedPipe(source, fromOffset, relOffset, toOffset, dir, projectedDir, LazyTypes(types.toArray), min,
                                    max, shouldExpandAll, pipeline,
                                    tempNodeOffset = tempNodeOffset,
                                    tempEdgeOffset = tempEdgeOffset,
                                    nodePredicate = buildPredicate(nodePredicate),
                                    edgePredicate = buildPredicate(edgePredicate),
                                    longsToCopy = incomingPipeline.numberOfLongs - sizeOfTemporaryStorage)(id)

      case Optional(inner, symbols) =>
        val nullableKeys = inner.availableSymbols -- symbols
        val nullableOffsets = nullableKeys.map(k => pipeline.getLongOffsetFor(k.name))
        OptionalSlottedPipe(source, nullableOffsets.toSeq, pipeline)(id)

      case Projection(_, expressions) =>
        val expressionsWithSlots = expressions map {
          case (k, e) =>
            val slot = pipeline.get(k).get
            slot -> convertExpressions(e)
        }
        ProjectionSlottedPipe(source, expressionsWithSlots)(id)

      case CreateNode(_, idName, labels, props) =>
        CreateNodeSlottedPipe(source, idName.name, pipeline, labels.map(LazyLabel.apply),
                               props.map(convertExpressions))(id)

      case MergeCreateNode(_, idName, labels, props) =>
        MergeCreateNodeSlottedPipe(source, idName.name, pipeline, labels.map(LazyLabel.apply), props.map(convertExpressions))(id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id)

      case UnwindCollection(_, IdName(name), expression) =>
        val offset = pipeline.getReferenceOffsetFor(name)
        UnwindSlottedPipe(source, expressionConverters.toCommandExpression(expression), offset, pipeline)(id)

      // Aggregation without grouping, such as RETURN count(*)
      case Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
              .asInstanceOf[AggregationExpression]
        }
        EagerAggregationWithoutGroupingSlottedPipe(source,
          pipeline,
          aggregation)(id)

      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        val grouping = groupingExpressions.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
        }
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
              .asInstanceOf[AggregationExpression]
        }
        EagerAggregationSlottedPipe(source,
                                     pipeline,
                                     grouping,
                                     aggregation)(id)

      case Distinct(_, groupingExpressions) =>
        val grouping = groupingExpressions.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
        }

        DistinctSlottedPipe(source, pipeline, grouping)(id)

      case CreateRelationship(_, idName, IdName(startNode), typ, IdName(endNode), props) =>
        val fromOffset = pipeline(startNode).offset
        val endOffset = pipeline(endNode).offset
        CreateRelationshipSlottedPipe(source, idName.name, fromOffset, LazyType(typ)(context.semanticTable), endOffset,
                                       pipeline, props.map(convertExpressions))(id = id)

      case MergeCreateRelationship(_, idName, IdName(startNode), typ, IdName(endNode), props) =>
        val fromOffset = pipeline(startNode).offset
        val endOffset = pipeline(endNode).offset
        MergeCreateRelationshipSlottedPipe(source, idName.name, fromOffset, LazyType(typ)(context.semanticTable),
                                            endOffset, pipeline, props.map(convertExpressions))(id = id)

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1SlottedPipe(source, sortItems.map(translateColumnOrder(pipeline, _)).toList)(id = id)

      case Top(_, sortItems, limit) =>
        TopNSlottedPipe(source, sortItems.map(translateColumnOrder(pipeline, _)).toList, convertExpressions(limit))(id = id)

      case Limit(_, count, IncludeTies) =>
        (source, count) match {
          case (SortSlottedPipe(inner, sortDescription, _), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesSlottedPipe(inner, sortDescription.toList)(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      // Pipes that do not themselves read/write slots should be fine to use the fallback (non-slot aware pipes)
      case _: Selection |
           _: Limit |
           _: ErrorPlan |
           _: Skip =>
        fallback.build(plan, source)

      case Sort(_, sortItems) =>
        SortSlottedPipe(source, sortItems.map(translateColumnOrder(pipeline, _)), pipeline)(id = id)

      case Eager(_) =>
        EagerSlottedPipe(source, pipeline)(id)

      case _ =>
        throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")
    }
  }

  private def translateColumnOrder(pipeline: PipelineInformation, s: plans.ColumnOrder): pipes.ColumnOrder = s match {
    case plans.Ascending(IdName(name)) => {
      pipeline.get(name) match {
        case Some(slot) => pipes.Ascending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    }
    case plans.Descending(IdName(name)) => {
      pipeline.get(name) match {
        case Some(slot) => pipes.Descending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    }
  }

  private def createProjectionsForResult(columns: Seq[String], pipelineInformation1: PipelineInformation) = {
    val runtimeColumns: Seq[(String, commandExpressions.Expression)] = columns map {
      k =>
        pipelineInformation1(k) match {
          case LongSlot(offset, false, CTNode, _) =>
            k -> slottedExpressions.NodeFromSlot(offset)
          case LongSlot(offset, true, CTNode, _) =>
            k -> slottedExpressions.NullCheck(offset, slottedExpressions.NodeFromSlot(offset))
          case LongSlot(offset, false, CTRelationship, _) =>
            k -> slottedExpressions.RelationshipFromSlot(offset)
          case LongSlot(offset, true, CTRelationship, _) =>
            k -> slottedExpressions.NullCheck(offset, slottedExpressions.RelationshipFromSlot(offset))

          case RefSlot(offset, _, _, _) =>
            k -> slottedExpressions.ReferenceFromSlot(offset)

          case _ =>
            throw new InternalException(s"Did not find `$k` in the pipeline information")
        }
    }
    runtimeColumns
  }

  private def buildPredicate(expr: frontEndAst.Expression)
                            (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): Predicate = {
    val rewrittenExpr: Expression = rewriteAstExpression(expr)

    expressionConverters.toCommandPredicate(rewrittenExpr).rewrite(KeyTokenResolver.resolveExpressions(_, planContext))
      .asInstanceOf[Predicate]
  }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = plan.assignedId
    val pipeline = pipelines(plan.assignedId)

    plan match {
      case Apply(_, _) =>
        ApplySlottedPipe(lhs, rhs)(id)

      case _: SemiApply |
           _: AntiSemiApply =>
        fallback.build(plan, lhs, rhs)

      case _: CartesianProduct =>
        val lhsPlan = plan.lhs.get
        val lhsPipeline = pipelines(lhsPlan.assignedId)
        CartesianProductSlottedPipe(lhs, rhs, lhsPipeline.numberOfLongs, lhsPipeline.numberOfReferences, pipeline)(id)

      case ConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => pipeline.get(idName.name) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => pipeline.getLongOffsetFor(e.name))
        val refOffsets = refIds.map(e => pipeline.getReferenceOffsetFor(e.name))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = false, pipeline)(id)

      case AntiConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => pipeline.get(idName.name) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => pipeline.getLongOffsetFor(e.name))
        val refOffsets = refIds.map(e => pipeline.getReferenceOffsetFor(e.name))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = true, pipeline)(id)

      case _ => throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")
    }
  }
}

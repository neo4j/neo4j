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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.{pipes => slottedPipes}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{IndexSeekModeFactory, LazyLabel, LazyTypes, Pipe, _}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.{expressions => slottedExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{LongSlot, PipeBuilder, PipeExecutionBuilderContext, PipelineInformation, _}
import org.neo4j.cypher.internal.compiler.v3_3.planner.{CantCompileQueryException, logical}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticTable, ast => frontEndAst}
import org.neo4j.cypher.internal.ir.v3_3.{IdName, VarPatternLength}

class SlottedPipeBuilder(fallback: PipeBuilder,
                         expressionConverters: ExpressionConverters,
                         idMap: Map[LogicalPlan, Id],
                         monitors: Monitors,
                         pipelines: Map[LogicalPlan, PipelineInformation],
                         readOnly: Boolean,
                         rewriteAstExpression: (frontEndAst.Expression) => frontEndAst.Expression)
                        (implicit context: PipeExecutionBuilderContext, planContext: PlanContext)
  extends PipeBuilder {

  private val convertExpressions: (frontEndAst.Expression) => commandExpressions.Expression =
    rewriteAstExpression andThen expressionConverters.toCommandExpression

  override def build(plan: LogicalPlan): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = idMap.getOrElse(plan, new Id)
    val pipelineInformation = pipelines(plan)

    plan match {
      case AllNodesScan(IdName(column), _) =>
        AllNodesScanRegisterPipe(column, pipelineInformation)(id)

      case NodeIndexScan(IdName(column), label, propertyKeys, _) =>
        NodeIndexScanRegisterPipe(column, label, propertyKeys, pipelineInformation)(id)

      case NodeIndexSeek(IdName(column), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekRegisterPipe(column, label, propertyKeys,
                                  valueExpr.map(convertExpressions), indexSeekMode, pipelineInformation)(id)

      case NodeUniqueIndexSeek(IdName(column), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekRegisterPipe(column, label, propertyKeys,
                                  valueExpr.map(convertExpressions), indexSeekMode, pipelineInformation)(id = id)

      case _: Argument =>
        ArgumentRegisterPipe(pipelineInformation)(id)

      case NodeByLabelScan(IdName(column), label, _) =>
        NodesByLabelScanRegisterPipe(column, LazyLabel(label), pipelineInformation)(id)

      case SingleRow() =>
        SingleRowRegisterPipe(pipelineInformation)(id)

      case _ =>
        throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")

    }
  }

  override def build(plan: LogicalPlan, source: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = idMap.getOrElse(plan, new Id)
    val pipeline = pipelines(plan)

    plan match {
      case ProduceResult(columns, _) =>
        val runtimeColumns = createProjectionsForResult(columns, pipeline)
        ProduceResultRegisterPipe(source, runtimeColumns)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
        val fromSlot = pipeline.getLongOffsetFor(from)
        val relSlot = pipeline.getLongOffsetFor(relName)
        val toSlot = pipeline.getLongOffsetFor(to)
        ExpandAllRegisterPipe(source, fromSlot, relSlot, toSlot, dir, LazyTypes(types), pipeline)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandInto) =>
        val fromOffset = pipeline.getLongOffsetFor(from)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(to)
        ExpandIntoRegisterPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types), pipeline)(id)

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandAll, predicates) =>
        val fromOffset = pipeline.getLongOffsetFor(fromName)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(toName)
        val predicate: Predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandAllRegisterPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types), predicate,
                                      pipeline)(id)

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandInto, predicates) =>
        val fromOffset = pipeline.getLongOffsetFor(fromName)
        val relOffset = pipeline.getLongOffsetFor(relName)
        val toOffset = pipeline.getLongOffsetFor(toName)
        val predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandIntoRegisterPipe(source, fromOffset, relOffset, toOffset, dir, LazyTypes(types), predicate,
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
        val incomingPipeline = pipelines(sourcePlan)
        val tempNodeOffset = incomingPipeline.getLongOffsetFor(tempNode)
        val tempEdgeOffset = incomingPipeline.getLongOffsetFor(tempEdge)
        val sizeOfTemporaryStorage = 2
        VarLengthExpandRegisterPipe(source, fromOffset, relOffset, toOffset, dir, projectedDir, LazyTypes(types), min,
                                    max, shouldExpandAll, pipeline,
                                    tempNodeOffset = tempNodeOffset,
                                    tempEdgeOffset = tempEdgeOffset,
                                    nodePredicate = buildPredicate(nodePredicate),
                                    edgePredicate = buildPredicate(edgePredicate),
                                    longsToCopy = incomingPipeline.numberOfLongs - sizeOfTemporaryStorage)(id)

      case Optional(inner, symbols) =>
        val nullableKeys = inner.availableSymbols -- symbols
        val nullableOffsets = nullableKeys.map(k => pipeline.getLongOffsetFor(k.name))
        OptionalRegisteredPipe(source, nullableOffsets.toSeq, pipeline)(id)

      case Projection(_, expressions) =>
        val expressionsWithSlots = expressions map {
          case (k, e) =>
            val slot = pipeline.get(k).get
            slot -> convertExpressions(e)
        }
        ProjectionRegisterPipe(source, expressionsWithSlots)(id)

      case CreateNode(_, idName, labels, props) =>
        CreateNodeRegisterPipe(source, idName.name, pipeline, labels.map(LazyLabel.apply),
                               props.map(convertExpressions))(id)

      case MergeCreateNode(_, idName, labels, props) =>
        MergeCreateNodeRegisterPipe(source, idName.name, pipeline, labels.map(LazyLabel.apply), props.map(convertExpressions))(id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id)

      case UnwindCollection(_, IdName(name), expression) =>
        val offset = pipeline.getReferenceOffsetFor(name)
        UnwindRegisterPipe(source, expressionConverters.toCommandExpression(expression), offset, pipeline)(id)

      // Aggregation without grouping, such as RETURN count(*)
      case Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
              .asInstanceOf[AggregationExpression]
        }
        EagerAggregationWithoutGroupingRegisterPipe(source,
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
        EagerAggregationRegisterPipe(source,
                                     pipeline,
                                     grouping,
                                     aggregation)(id)

      case Distinct(_, groupingExpressions) =>
        val grouping = groupingExpressions.map {
          case (key, expression) =>
            pipeline.getReferenceOffsetFor(key) -> expressionConverters.toCommandExpression(expression)
        }

        DistinctRegisterPipe(source, pipeline, grouping)(id)

      case CreateRelationship(_, idName, IdName(startNode), typ, IdName(endNode), props) =>
        val fromOffset = pipeline(startNode).offset
        val endOffset = pipeline(endNode).offset
        CreateRelationshipRegisterPipe(source, idName.name, fromOffset, LazyType(typ)(context.semanticTable), endOffset,
                                       pipeline, props.map(convertExpressions))(id = id)

      case MergeCreateRelationship(_, idName, IdName(startNode), typ, IdName(endNode), props) =>
        val fromOffset = pipeline(startNode).offset
        val endOffset = pipeline(endNode).offset
        MergeCreateRelationshipRegisterPipe(source, idName.name, fromOffset, LazyType(typ)(context.semanticTable),
                                            endOffset, pipeline, props.map(convertExpressions))(id = id)

      // Pipes that do not themselves read/write registers/slots should be fine to use the fallback (non-register aware pipes)
      case _: Selection |
           _: Limit |
           _: ErrorPlan |
           _: Skip =>
        fallback.build(plan, source)

      case Sort(_, sortItems) =>
        SortRegisterPipe(source, sortItems.map(translateColumnOrder(pipeline, _)), pipeline)(id = id)

      case _ =>
        throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")
    }
  }

  private def translateColumnOrder(pipeline: PipelineInformation, s: logical.SortDescription): slottedPipes.ColumnOrder = s match {
    case logical.Ascending(IdName(name)) => {
      pipeline.get(name) match {
        case Some(slot) => slottedPipes.Ascending(slot.offset)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    }
    case logical.Descending(IdName(name)) => {
      pipeline.get(name) match {
        case Some(slot) => slottedPipes.Descending(slot.offset)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    }
  }

  private def createProjectionsForResult(columns: Seq[String], pipelineInformation1: PipelineInformation) = {
    val runtimeColumns: Seq[(String, commandExpressions.Expression)] = columns map {
      k =>
        pipelineInformation1(k) match {
          case LongSlot(offset, false, CTNode, _) =>
            k -> slottedExpressions.NodeFromRegister(offset)
          case LongSlot(offset, true, CTNode, _) =>
            k -> slottedExpressions.NullCheck(offset, slottedExpressions.NodeFromRegister(offset))
          case LongSlot(offset, false, CTRelationship, _) =>
            k -> slottedExpressions.RelationshipFromRegister(offset)
          case LongSlot(offset, true, CTRelationship, _) =>
            k -> slottedExpressions.NullCheck(offset, slottedExpressions.RelationshipFromRegister(offset))

          case RefSlot(offset, _, _, _) =>
            k -> slottedExpressions.ReferenceFromRegister(offset)

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

    val id = idMap.getOrElse(plan, new Id)
    val pipeline = pipelines(plan)

    plan match {
      case Apply(_, _) =>
        ApplyRegisterPipe(lhs, rhs)(id)

      case _: SemiApply |
           _: AntiSemiApply =>
        fallback.build(plan, lhs, rhs)

      case _: CartesianProduct =>
        val lhsPlan = plan.lhs.get
        val lhsLongCount = pipelines(lhsPlan).numberOfLongs
        val lhsRefCount = pipelines(lhsPlan).numberOfReferences
        CartesianProductRegisterPipe(lhs, rhs, lhsLongCount, lhsRefCount, pipeline)(id)

      case ConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => pipeline.get(idName.name) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
        })
        val longOffsets = longIds.map(e => pipeline.getLongOffsetFor(e.name))
        val refOffsets = refIds.map(e => pipeline.getReferenceOffsetFor(e.name))
        ConditionalApplyRegisterPipe(lhs, rhs, longOffsets, refOffsets, negated = false, pipeline)(id)

      case AntiConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => pipeline.get(idName.name) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
        })
        val longOffsets = longIds.map(e => pipeline.getLongOffsetFor(e.name))
        val refOffsets = refIds.map(e => pipeline.getReferenceOffsetFor(e.name))
        ConditionalApplyRegisterPipe(lhs, rhs, longOffsets, refOffsets, negated = true, pipeline)(id)

      case _ => throw new CantCompileQueryException(s"Unsupported logical plan operator: $plan")
    }
  }
}

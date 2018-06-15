/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.RefSlot
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexSeekModeFactory, LazyLabel, LazyTypes}
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeBuilder.translateColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.expressions.AggregationExpressionOperator
import org.neo4j.cypher.internal.runtime.vectorized.operators._
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.util.InternalException

class PipelineBuilder(physicalPlan: PhysicalPlan, converters: ExpressionConverters, readOnly: Boolean)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    val pipeline: Pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def build(plan: LogicalPlan): Pipeline = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val argumentSize = physicalPlan.argumentSizes(plan.id)

    val thisOp = plan match {
      case plans.AllNodesScan(column, _) =>
        new AllNodeScanOperator(
          slots.getLongOffsetFor(column),
          argumentSize)

      case plans.NodeByLabelScan(column, label, _) =>
        new LabelScanOperator(
          slots.getLongOffsetFor(column),
          LazyLabel(label)(SemanticTable()),
          argumentSize)

      case plans.NodeIndexScan(column, labelToken, propertyKey, _) =>
        new NodeIndexScanOperator(
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          propertyKey.nameId.id,
          argumentSize)

      case NodeIndexContainsScan(column, labelToken, propertyKey, valueExpr, _) =>
        new NodeIndexContainsScanOperator(
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          propertyKey.nameId.id,
          converters.toCommandExpression(valueExpr),
          argumentSize)

      case plans.NodeIndexSeek(column, label, propertyKeys, valueExpr,  _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(
          slots.getLongOffsetFor(column),
          label,
          propertyKeys,
          argumentSize,
          valueExpr.map(converters.toCommandExpression),
          indexSeekMode)

      case plans.NodeUniqueIndexSeek(column, label, propertyKeys, valueExpr,  _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(
          slots.getLongOffsetFor(column),
          label,
          propertyKeys,
          argumentSize,
          valueExpr.map(converters.toCommandExpression),
          indexSeekMode)

      case plans.Argument(_) =>
        new ArgumentOperator(argumentSize)

      case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
    }

    new RegularPipeline(thisOp, slots, None)
  }

  override protected def build(plan: LogicalPlan, from: Pipeline): Pipeline = {
    var source = from
    val slots = physicalPlan.slotConfigurations(plan.id)

      val thisOp = plan match {
        case plans.ProduceResult(_, columns) =>
          new ProduceResultOperator(slots, columns.toArray)

        case plans.Selection(predicate, _) =>
          new FilterOperator(converters.toCommandPredicate(predicate))

        case plans.Expand(lhs, fromName, dir, types, to, relName, ExpandAll) =>
          val fromOffset = slots.getLongOffsetFor(fromName)
          val relOffset = slots.getLongOffsetFor(relName)
          val toOffset = slots.getLongOffsetFor(to)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(fromOffset, relOffset, toOffset, dir, lazyTypes)

        case plans.Projection(_, expressions) =>
          val projectionOps = expressions.map {
            case (key, e) => slots(key) -> converters.toCommandExpression(e)
          }
          new ProjectOperator(projectionOps)

        case plans.Sort(_, sortItems) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val preSorting = new PreSortOperator(ordering)
          source.addOperator(preSorting)
          new MergeSortOperator(ordering)

        case Top(_, sortItems, limit) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val countExpression = converters.toCommandExpression(limit)
          val preTop = new PreSortOperator(ordering, Some(countExpression))
          source.addOperator(preTop)
          new MergeSortOperator(ordering, Some(countExpression))

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset,
                                 converters.toCommandExpression(expression).asInstanceOf[AggregationExpressionOperator])
          }.toArray

          //add mapper to source
          source.addOperator(new AggregationMapperOperatorNoGrouping(aggregations))
          new AggregationReduceOperatorNoGrouping(aggregations)

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
          val groupings = groupingExpressions.map {
            case (key, expression) =>
              val currentSlot = slots(key)
              //we need to make room for storing grouping value in source slot
              if (currentSlot.isLongSlot)
                source.slots.newLong(key, currentSlot.nullable, currentSlot.typ)
              else
                source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              GroupingOffsets(source.slots(key), currentSlot, converters.toCommandExpression(expression))
          }.toArray

          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset,
                                 converters.toCommandExpression(expression).asInstanceOf[AggregationExpressionOperator])
          }.toArray

          //add mapper to source
          source.addOperator(new AggregationMapperOperator(aggregations, groupings))
          new AggregationReduceOperator(aggregations, groupings)

        case plans.UnwindCollection(src, variable, collection) =>
          val offset = slots.get(variable) match {
            case Some(RefSlot(idx, _, _)) => idx
            case _ =>
              throw new InternalException("Weird slot found for UNWIND")
          }
          val runtimeExpression = converters.toCommandExpression(collection)
          new UnwindOperator(runtimeExpression, offset)

        case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
      }

    thisOp match {
      case o: Operator =>
        new RegularPipeline(o, slots, Some(source))
      case mo: StatelessOperator =>
        source.addOperator(mo)
        source
      case ro: ReduceOperator =>
        new ReducePipeline(ro, slots, Some(source))
    }
  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {
    val slots = physicalPlan.slotConfigurations(plan.id)

    throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
  }
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}

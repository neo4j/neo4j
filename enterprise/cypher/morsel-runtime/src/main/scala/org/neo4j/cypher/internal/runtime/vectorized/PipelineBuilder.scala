/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.RefSlot
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, LazyTypes}
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeBuilder.translateColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.expressions.AggregationExpressionOperator
import org.neo4j.cypher.internal.runtime.vectorized.operators._
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._

class PipelineBuilder(slotConfigurations: SlotConfigurations, converters: ExpressionConverters)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    val pipeline: Pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def build(plan: LogicalPlan): Pipeline = {
    val slots = slotConfigurations(plan.id)

    val thisOp = plan match {
      case plans.AllNodesScan(column, _) =>
        new AllNodeScanOperator(
          slots.numberOfLongs,
          slots.numberOfReferences,
          slots.getLongOffsetFor(column))

      case plans.NodeByLabelScan(column, label, _) =>
        new LabelScanOperator(
          slots.numberOfLongs,
          slots.numberOfReferences,
          slots.getLongOffsetFor(column),
          LazyLabel(label)(SemanticTable()))

      case plans.NodeIndexSeek(column, label, propertyKeys, SingleQueryExpression(valueExpr),  _) if propertyKeys.size == 1 =>
        new NodeIndexSeekOperator(
          slots.numberOfLongs,
          slots.numberOfReferences,
          slots.getLongOffsetFor(column),
          label, propertyKeys.head, converters.toCommandExpression(valueExpr))

      case plans.NodeUniqueIndexSeek(column, label, propertyKeys, SingleQueryExpression(valueExpr),  _) if propertyKeys.size == 1 =>
        new NodeIndexSeekOperator(
          slots.numberOfLongs,
          slots.numberOfReferences,
          slots.getLongOffsetFor(column),
          label, propertyKeys.head, converters.toCommandExpression(valueExpr))

      case plans.Argument(_) =>
        new ArgumentOperator

      case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
    }

    Pipeline(thisOp, IndexedSeq.empty, slots, NoDependencies)()
  }

  override protected def build(plan: LogicalPlan, from: Pipeline): Pipeline = {
    var source = from
    val slots = slotConfigurations(plan.id)

      val thisOp = plan match {
        case plans.ProduceResult(_, columns) =>
          new ProduceResultOperator(slots, columns.toArray)

        case plans.Selection(predicates, _) =>
          val predicate = predicates.map(converters.toCommandPredicate).reduce(_ andWith _)
          new FilterOperator(slots, predicate)

        case plans.Expand(lhs, fromName, dir, types, to, relName, ExpandAll) =>
          val fromOffset = slots.getLongOffsetFor(fromName)
          val relOffset = slots.getLongOffsetFor(relName)
          val toOffset = slots.getLongOffsetFor(to)
          val fromPipe = slotConfigurations(lhs.id)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(slots, fromPipe, fromOffset, relOffset, toOffset, dir, lazyTypes)

        case plans.Projection(_, expressions) =>
          val projectionOps = expressions.map {
            case (key, e) => slots(key) -> converters.toCommandExpression(e)
          }
          new ProjectOperator(projectionOps, slots)

        case plans.Sort(_, sortItems) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val preSorting = new PreSortOperator(ordering, slots)
          source = source.addOperator(preSorting)
          new MergeSortOperator(ordering, slots)

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
          source = source.addOperator(new AggregationMapperOperatorNoGrouping(source.slots, aggregations))
          new AggregationReduceOperatorNoGrouping(slots, aggregations)

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
          val grouping = groupingExpressions.map {
            case (key, expression) =>
              val currentSlot = slots(key)
              //we need to make room for storing grouping value in source slot
              if (currentSlot.isLongSlot) source.slots.newLong(key, currentSlot.nullable, currentSlot.typ)
              else source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
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
          source = source.addOperator(new AggregationMapperOperator(source.slots, aggregations, grouping))
          new AggregationReduceOperator(slots, aggregations, grouping)

        case plans.UnwindCollection(src, variable, collection) =>
          val offset = slots.get(variable) match {
            case Some(RefSlot(idx, _, _)) => idx
            case _ =>
              throw new InternalException("Weird slot found for UNWIND")
          }
          val runtimeExpression = converters.toCommandExpression(collection)
          new UnwindOperator(runtimeExpression, offset, slotConfigurations(src.id), slots)

        case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
      }

    thisOp match {
      case o: Operator =>
        Pipeline(o, IndexedSeq.empty, slots, o.addDependency(source))()
      case mo: MiddleOperator =>
        source.addOperator(mo)
    }
  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline =
    throw new CantCompileQueryException(s"$plan is not supported in morsel runtime")
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}

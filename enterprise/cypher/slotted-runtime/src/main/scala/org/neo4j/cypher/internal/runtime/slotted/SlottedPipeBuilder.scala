/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast.{NodeFromSlot, RelationshipFromSlot}
import org.neo4j.cypher.internal.ir.v3_5.VarPatternLength
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{KeyTokenResolver, expressions => commandExpressions}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{DropResultPipe, ColumnOrder => _, _}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, InterpretedPipeBuilder}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.{expressions => slottedExpressions}
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions.{Equals, SignedDecimalIntegerLiteral}
import org.opencypher.v9_0.util.AssertionUtils._
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.{expressions => frontEndAst}

class SlottedPipeBuilder(fallback: PipeBuilder,
                         expressionConverters: ExpressionConverters,
                         physicalPlan: PhysicalPlan,
                         readOnly: Boolean,
                         rewriteAstExpression: frontEndAst.Expression => frontEndAst.Expression)
                        (implicit context: PipeExecutionBuilderContext, tokenContext: TokenContext)
  extends PipeBuilder {

  private val convertExpressions: frontEndAst.Expression => commandExpressions.Expression =
    rewriteAstExpression andThen expressionConverters.toCommandExpression

  override def onLeaf(plan: LogicalPlan): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = plan.id
    val slots = physicalPlan.slotConfigurations(plan.id)
    val argumentSize = physicalPlan.argumentSizes(plan.id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case AllNodesScan(column, _) =>
        AllNodesScanSlottedPipe(column, slots, argumentSize)(id)

      case NodeIndexScan(column, label, propertyKeys, _) =>
        NodeIndexScanSlottedPipe(column, label, propertyKeys, slots, argumentSize)(id)

      case NodeIndexSeek(column, label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, propertyKeys,
          valueExpr.map(convertExpressions), indexSeekMode, slots, argumentSize)(id)

      case NodeUniqueIndexSeek(column, label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, propertyKeys,
          valueExpr.map(convertExpressions), indexSeekMode, slots, argumentSize)(id = id)

      case NodeByLabelScan(column, label, _) =>
        NodesByLabelScanSlottedPipe(column, LazyLabel(label), slots, argumentSize)(id)

      case _: Argument =>
        ArgumentSlottedPipe(slots, argumentSize)(id)

      case _ =>
        fallback.onLeaf(plan)
    }
    pipe.setExecutionContextFactory(SlottedExecutionContextFactory(slots))
    pipe
  }

  private def generateSlotAccessorFunctions(slots: SlotConfiguration) = {
    slots.foreachSlot {
      case (key, slot) =>
        val getter = SlottedPipeBuilderUtils.makeGetValueFromSlotFunctionFor(slot)
        val setter = SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot)
        val primitiveNodeSetter =
          if (slot.typ.isAssignableFrom(CTNode))
            Some(SlottedPipeBuilderUtils.makeSetPrimitiveNodeInSlotFunctionFor(slot))
          else
            None
        val primitiveRelationshipSetter =
          if (slot.typ.isAssignableFrom(CTRelationship))
            Some(SlottedPipeBuilderUtils.makeSetPrimitiveRelationshipInSlotFunctionFor(slot))
          else
            None

        slots.updateAccessorFunctions(key, getter, setter, primitiveNodeSetter, primitiveRelationshipSetter)
    }
  }

  override def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = plan.id
    val slots = physicalPlan.slotConfigurations(plan.id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case ProduceResult(_, columns) =>
        val runtimeColumns = createProjectionsForResult(columns, slots)
        ProduceResultSlottedPipe(source, runtimeColumns)(id)

      case Expand(_, from, dir, types, to, relName, ExpandAll) =>
        val fromSlot = slots(from)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        ExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, LazyTypes(types.toArray), slots)(id)

      case Expand(_, from, dir, types, to, relName, ExpandInto) =>
        val fromSlot = slots(from)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(to)
        ExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, LazyTypes(types.toArray), slots)(id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandAll, predicates) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(toName)
        val predicate: Predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, LazyTypes(types.toArray), predicate,
          slots)(id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandInto, predicates) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(toName)
        val predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, LazyTypes(types.toArray), predicate,
          slots)(id)

      case VarExpand(sourcePlan, fromName, dir, projectedDir, types, toName, relName,
      VarPatternLength(min, max), expansionMode, tempNode, tempEdge, nodePredicate,
      edgePredicate, _) =>
        val shouldExpandAll = expansionMode match {
          case ExpandAll => true
          case ExpandInto => false
        }
        val fromSlot = slots(fromName)
        val relOffset = slots.getReferenceOffsetFor(relName)
        val toSlot = slots(toName)

        // The node/edge predicates are evaluated on the source pipeline, not the produced one
        val sourceSlots = physicalPlan.slotConfigurations(sourcePlan.id)
        val tempNodeOffset = sourceSlots.getLongOffsetFor(tempNode)
        val tempEdgeOffset = sourceSlots.getLongOffsetFor(tempEdge)
        val argumentSize = SlotConfiguration.Size(sourceSlots.numberOfLongs - 2, sourceSlots.numberOfReferences)
        VarLengthExpandSlottedPipe(source, fromSlot, relOffset, toSlot, dir, projectedDir, LazyTypes(types.toArray), min,
          max, shouldExpandAll, slots,
          tempNodeOffset = tempNodeOffset,
          tempEdgeOffset = tempEdgeOffset,
          nodePredicate = buildPredicate(nodePredicate),
          edgePredicate = buildPredicate(edgePredicate),
          argumentSize = argumentSize)(id)

      case Optional(inner, symbols) =>
        val nullableKeys = inner.availableSymbols -- symbols
        val nullableSlots: Array[Slot] = nullableKeys.map(k => slots.get(k).get).toArray
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        OptionalSlottedPipe(source, nullableSlots, slots, argumentSize)(id)

      case Projection(_, expressions) =>
        val expressionsWithSlots: Map[Int, commandExpressions.Expression] = expressions collect {
          case (k, e) if refSlotAndNotAlias(slots, k) =>
            val slot = slots.get(k).get
            slot.offset -> convertExpressions(e)
        }
        ProjectionSlottedPipe(source, expressionsWithSlots)(id)

      case Create(_, nodes, relationships) =>
        CreateSlottedPipe(
          source,
          nodes.map(n =>
            CreateNodeSlottedCommand(
              slots.getLongOffsetFor(n.idName),
              n.labels.map(LazyLabel.apply),
              n.properties.map(convertExpressions)
            )
          ).toIndexedSeq,
          relationships.map(r =>
            CreateRelationshipSlottedCommand(
              slots.getLongOffsetFor(r.idName),
              SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(r.startNode)),
              LazyType(r.relType.name),
              SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(r.endNode)),
              r.properties.map(convertExpressions))
          ).toIndexedSeq
        )(id)

      case MergeCreateNode(_, idName, labels, properties) =>
        MergeCreateNodeSlottedPipe(
          source,
          CreateNodeSlottedCommand(
            slots.getLongOffsetFor(idName),
            labels.map(LazyLabel.apply),
            properties.map(convertExpressions)
          )
        )(id)

      case MergeCreateRelationship(_, idName, startNode, relType, endNode, properties) =>
        MergeCreateRelationshipSlottedPipe(
          source,
          CreateRelationshipSlottedCommand(
            slots.getLongOffsetFor(idName),
            SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(startNode)),
            LazyType(relType.name),
            SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(endNode)),
            properties.map(convertExpressions)
          )
        )(id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id)

      case DropResult(_) =>
        DropResultPipe(source)(id)

      case UnwindCollection(_, name, expression) =>
        val offset = slots.getReferenceOffsetFor(name)
        UnwindSlottedPipe(source, convertExpressions(expression), offset, slots)(id)

      // Aggregation without grouping, such as RETURN count(*)
      case Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            slots.getReferenceOffsetFor(key) -> convertExpressions(expression)
              .asInstanceOf[AggregationExpression]
        }
        EagerAggregationWithoutGroupingSlottedPipe(source, slots, aggregation)(id)

      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            slots.getReferenceOffsetFor(key) -> convertExpressions(expression)
              .asInstanceOf[AggregationExpression]
        }

        val groupingColumnsIncoming: Array[Int] = groupingExpressions.values.collect {
          case NodeFromSlot(offset, _) => offset
          case RelationshipFromSlot(offset, _) => offset
        }.toArray

        val groupingColumnsOutgoing: Array[Int] = groupingExpressions.keys.collect {
          case x if slots(x).isLongSlot => slots(x).offset
        }.toArray

        if (groupingColumnsIncoming.length == groupingExpressions.size &&
          groupingColumnsIncoming.length == groupingColumnsOutgoing.length) {
          // If we are able to use primitive for all incoming and outgoing grouping columns, we can use the more effective
          // Primitive pipe that leverages that the fact that grouping can be done a single array of longs
          EagerAggregationSlottedPrimitivePipe(source, slots, groupingColumnsIncoming, groupingColumnsOutgoing, aggregation)(id)
        } else {
          val grouping = groupingExpressions.map {
            case (key, expression) =>
              slots(key) -> convertExpressions(expression)
          }
          EagerAggregationSlottedPipe(source, slots, grouping, aggregation)(id)
        }

      case Distinct(_, groupingExpressions) =>
        chooseDistinctPipe(groupingExpressions, slots, source, id)

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1SlottedPipe(source, sortItems.map(translateColumnOrder(slots, _)).toList)(id = id)

      case Top(_, sortItems, limit) =>
        TopNSlottedPipe(source, sortItems.map(translateColumnOrder(slots, _)).toList, convertExpressions(limit))(id = id)

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
        fallback.onOneChildPlan(plan, source)

      case Sort(_, sortItems) =>
        SortSlottedPipe(source, sortItems.map(translateColumnOrder(slots, _)), slots)(id = id)

      case Eager(_) =>
        EagerSlottedPipe(source, slots)(id)

      case _: DeleteNode |
           _: DeleteRelationship |
           _: DeletePath |
           _: DeleteExpression |
           _: DetachDeleteNode |
           _: DetachDeletePath |
           _: DetachDeleteExpression =>
        fallback.onOneChildPlan(plan, source)

      case _: SetLabels |
           _: SetNodeProperty |
           _: SetNodePropertiesFromMap |
           _: SetRelationshipPropery |
           _: SetRelationshipPropertiesFromMap |
           _: SetProperty |
           _: RemoveLabels =>
        fallback.onOneChildPlan(plan, source)

      case _: LockNodes =>
        fallback.onOneChildPlan(plan, source)

      case _ =>
        fallback.onOneChildPlan(plan, source)
    }
    pipe.setExecutionContextFactory(SlottedExecutionContextFactory(slots))
    pipe
  }

  private def refSlotAndNotAlias(slots: SlotConfiguration, k: String) = {
    !slots.isAlias(k) &&
      slots.get(k).forall(_.isInstanceOf[RefSlot])
  }

  private def translateColumnOrder(slots: SlotConfiguration, s: plans.ColumnOrder): pipes.ColumnOrder = s match {
    case plans.Ascending(name) =>
      slots.get(name) match {
        case Some(slot) => pipes.Ascending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the slot configuration")
      }

    case plans.Descending(name) =>
      slots.get(name) match {
        case Some(slot) => pipes.Descending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the slot configuration")
      }
  }

  private def createProjectionsForResult(columns: Seq[String], slots: SlotConfiguration) = {
    val runtimeColumns: Seq[(String, commandExpressions.Expression)] =
      columns.map(createProjectionForIdentifier(slots))
    runtimeColumns
  }

  private def createProjectionForIdentifier(slots: SlotConfiguration)(identifier: String) = {
    val slot = slots.get(identifier).getOrElse(
      throw new InternalException(s"Did not find `$identifier` in the slot configuration")
    )
    identifier -> SlottedPipeBuilder.projectSlotExpression(slot)
  }

  private def buildPredicate(expr: frontEndAst.Expression)
                            (implicit context: PipeExecutionBuilderContext): Predicate = {
    val rewrittenExpr: frontEndAst.Expression = rewriteAstExpression(expr)

    expressionConverters
      .toCommandPredicate(rewrittenExpr)
      .rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext))
      .asInstanceOf[Predicate]
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val slotConfigs = physicalPlan.slotConfigurations
    val id = plan.id
    val slots = slotConfigs(plan.id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case Apply(_, _) =>
        ApplySlottedPipe(lhs, rhs)(id)

      case _: AbstractSemiApply |
           _: AbstractSelectOrSemiApply =>
        fallback.onTwoChildPlan(plan, lhs, rhs)

      case RollUpApply(_, rhsPlan, collectionName, identifierToCollect, nullables) =>
        val rhsSlots = slotConfigs(rhsPlan.id)
        val identifierToCollectExpression = createProjectionForIdentifier(rhsSlots)(identifierToCollect)
        val collectionRefSlotOffset = slots.getReferenceOffsetFor(collectionName)
        RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset, identifierToCollectExpression,
          nullables, slots)(id = id)

      case _: CartesianProduct =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val lhsPlan = plan.lhs.get
        val lhsSlots = slotConfigs(lhsPlan.id)

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within CartesianProductSlottedPipe.
        ifAssertionsEnabled(verifyOnlyArgumentsAreSharedSlots(plan, physicalPlan))

        CartesianProductSlottedPipe(lhs, rhs, lhsSlots.numberOfLongs, lhsSlots.numberOfReferences, slots, argumentSize)(id)

      case joinPlan: NodeHashJoin =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val leftNodes: Array[Int] = joinPlan.nodes.map(k => slots.getLongOffsetFor(k)).toArray
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rightNodes: Array[Int] = joinPlan.nodes.map(k => rhsSlots.getLongOffsetFor(k)).toArray
        val copyLongsFromRHS = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]
        val copyRefsFromRHS = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]

        // Verify the assumption that the argument slots are the same on both sides
        ifAssertionsEnabled(verifyArgumentsAreTheSameOnBothSides(plan, physicalPlan))

        // When executing the HashJoin, the LHS will be copied to the first slots in the produced row, and any additional RHS columns that are not
        // part of the join comparison
        rhsSlots.foreachSlotOrdered {
          case (key, LongSlot(offset, _, _)) if offset >= argumentSize.nLongs =>
            copyLongsFromRHS += ((offset, slots.getLongOffsetFor(key)))
          case (key, RefSlot(offset, _, _)) if offset >= argumentSize.nReferences =>
            copyRefsFromRHS += ((offset, slots.getReferenceOffsetFor(key)))
          case _ => // do nothing, already added by lhs
        }
        NodeHashJoinSlottedPipe(leftNodes, rightNodes, lhs, rhs, slots, copyLongsFromRHS.result().toArray, copyRefsFromRHS.result().toArray)(id)

      case ValueHashJoin(lhsPlan, _, Equals(lhsAstExp, rhsAstExp)) =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val lhsCmdExp = convertExpressions(lhsAstExp)
        val rhsCmdExp = convertExpressions(rhsAstExp)
        val lhsSlots = slotConfigs(lhsPlan.id)
        val longOffset = lhsSlots.numberOfLongs
        val refOffset = lhsSlots.numberOfReferences

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within CartesianProductSlottedPipe.
        ifAssertionsEnabled(verifyOnlyArgumentsAreSharedSlots(plan, physicalPlan))

        ValueHashJoinSlottedPipe(lhsCmdExp, rhsCmdExp, lhs, rhs, slots, longOffset, refOffset, argumentSize)(id)

      case ConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = false, slots)(id)

      case AntiConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(s: LongSlot) => true
          case Some(s: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = true, slots)(id)

      case ForeachApply(_, _, variable, expression) =>
        val innerVariableSlot = slots.get(variable).getOrElse(throw new InternalException(s"Foreach variable '$variable' has no slot"))
        ForeachSlottedPipe(lhs, rhs, innerVariableSlot, convertExpressions(expression))(id)

      case Union(_, _) =>
        val lhsSlots = slotConfigs(lhs.id)
        val rhsSlots = slotConfigs(rhs.id)
        UnionSlottedPipe(lhs, rhs,
          SlottedPipeBuilder.computeUnionMapping(lhsSlots, slots),
          SlottedPipeBuilder.computeUnionMapping(rhsSlots, slots))(id = id)

      case _: AssertSameNode =>
        fallback.onTwoChildPlan(plan, lhs, rhs)

      case _ =>
        fallback.onTwoChildPlan(plan, lhs, rhs)
    }
    pipe.setExecutionContextFactory(SlottedExecutionContextFactory(slots))
    pipe
  }

  private def chooseDistinctPipe(groupingExpressions: Map[String, frontEndAst.Expression],
                                 slots: SlotConfiguration,
                                 source: Pipe,
                                 id: Id): Pipe = {

    /**
      * We use these objects to figure out:
      * a) can we use the primitive distinct pipe?
      * b) if we can, what offsets are interesting
      */
    trait DistinctPhysicalOp {
      def addExpression(e: frontEndAst.Expression): DistinctPhysicalOp
    }

    case class AllPrimitive(offsets: Seq[Int]) extends DistinctPhysicalOp {
      override def addExpression(e: frontEndAst.Expression): DistinctPhysicalOp = e match {
        case v: NodeFromSlot =>
          AllPrimitive(offsets :+ v.offset)
        case v: RelationshipFromSlot =>
          AllPrimitive(offsets :+ v.offset)
        case _ =>
          References
      }
    }

    object References extends DistinctPhysicalOp {
      override def addExpression(e: frontEndAst.Expression): DistinctPhysicalOp = References
    }

    val runtimeProjections: Map[Slot, commandExpressions.Expression] = groupingExpressions.map {
      case (key, expression) =>
        slots(key) -> convertExpressions(expression)
    }

    val physicalDistinctOp = groupingExpressions.foldLeft[DistinctPhysicalOp](AllPrimitive(Seq.empty)) {
      case (acc: DistinctPhysicalOp, (_, expression)) =>
        acc.addExpression(expression)
    }

    physicalDistinctOp match {
      case AllPrimitive(offsets) if offsets.size == 1 =>
        val (toSlot, runtimeExpression) = runtimeProjections.head
        DistinctSlottedSinglePrimitivePipe(source, slots, toSlot, offsets.head, runtimeExpression)(id)

      case AllPrimitive(offsets) =>
        DistinctSlottedPrimitivePipe(source, slots, offsets.sorted.toArray, runtimeProjections)(id)

      case References =>
        DistinctSlottedPipe(source, slots, runtimeProjections)(id)
    }
  }

  // Verifies the assumption that all shared slots are arguments with slot offsets within the first argument size number of slots
  // and the number of shared slots are identical to the argument size.
  private def verifyOnlyArgumentsAreSharedSlots(plan: LogicalPlan, physicalPlan: PhysicalPlan) = {
    val argumentSize = physicalPlan.argumentSizes(plan.id)
    val lhsPlan = plan.lhs.get
    val rhsPlan = plan.rhs.get
    val lhsSlots = physicalPlan.slotConfigurations(lhsPlan.id)
    val rhsSlots = physicalPlan.slotConfigurations(rhsPlan.id)
    val (sharedSlots, rhsUniqueSlots) = rhsSlots.partitionSlots {
      case (k, slot) =>
        lhsSlots.get(k).isDefined
    }
    val (sharedLongSlots, sharedRefSlots) = sharedSlots.partition(_._2.isLongSlot)

    val longSlotsOk = sharedLongSlots.forall {
      case (key, slot) => slot.offset < argumentSize.nLongs
    } && sharedLongSlots.size == argumentSize.nLongs

    val refSlotsOk = sharedRefSlots.forall {
      case (key, slot) => slot.offset < argumentSize.nReferences
    } && sharedRefSlots.size == argumentSize.nReferences

    if (!longSlotsOk || !refSlotsOk) {
      val longSlotsMessage = if (longSlotsOk) "" else s"#long arguments=${argumentSize.nLongs} shared long slots: $sharedLongSlots "
      val refSlotsMessage = if (refSlotsOk) "" else s"#ref arguments=${argumentSize.nReferences} shared ref slots: $sharedRefSlots "
      throw new InternalException(s"Unexpected slot configuration. Shared slots not only within argument size: $longSlotsMessage$refSlotsMessage")
    }
  }

  private def verifyArgumentsAreTheSameOnBothSides(plan: LogicalPlan, physicalPlan: PhysicalPlan) = {
    val argumentSize = physicalPlan.argumentSizes(plan.id)
    val lhsPlan = plan.lhs.get
    val rhsPlan = plan.rhs.get
    val lhsSlots = physicalPlan.slotConfigurations(lhsPlan.id)
    val rhsSlots = physicalPlan.slotConfigurations(rhsPlan.id)
    val (lhsLongSlots, lhsRefSlots) = lhsSlots.partitionSlots((_, slot) => slot.isLongSlot)
    val (rhsLongSlots, rhsRefSlots) = rhsSlots.partitionSlots((_, slot) => slot.isLongSlot)

    val lhsArgLongSlots = lhsLongSlots.filter { case (_, slot) => slot.offset < argumentSize.nLongs } sortBy(_._1)
    val lhsArgRefSlots = lhsRefSlots.filter { case (_, slot) => slot.offset < argumentSize.nReferences } sortBy(_._1)
    val rhsArgLongSlots = rhsLongSlots.filter { case (_, slot) => slot.offset < argumentSize.nLongs } sortBy(_._1)
    val rhsArgRefSlots = rhsRefSlots.filter { case (_, slot) => slot.offset < argumentSize.nReferences } sortBy(_._1)

    val sizesAreTheSame =
      lhsArgLongSlots.size == rhsArgLongSlots.size && lhsArgLongSlots.size == argumentSize.nLongs &&
        lhsArgRefSlots.size == rhsArgRefSlots.size && lhsArgRefSlots.size == argumentSize.nReferences

    def sameSlotsInOrder(a: Seq[(String, Slot)], b: Seq[(String, Slot)]): Boolean =
      a.zip(b) forall {
        case ((k1, slot1), (k2, slot2)) =>
          k1 == k2 && slot1.offset == slot2.offset && slot1.isTypeCompatibleWith(slot2)
      }

    val longSlotsOk = sizesAreTheSame && sameSlotsInOrder(lhsArgLongSlots, rhsArgLongSlots)
    val refSlotsOk = sizesAreTheSame && sameSlotsInOrder(lhsArgRefSlots, rhsArgRefSlots)

    if (!longSlotsOk || !refSlotsOk) {
      val longSlotsMessage = if (longSlotsOk) "" else s"#long arguments=${argumentSize.nLongs} lhs: $lhsLongSlots rhs: $rhsArgLongSlots "
      val refSlotsMessage = if (refSlotsOk) "" else s"#ref arguments=${argumentSize.nReferences} lhs: $lhsRefSlots rhs: $rhsArgRefSlots "
      throw new InternalException(s"Unexpected slot configuration. Arguments differ between lhs and rhs: $longSlotsMessage$refSlotsMessage")
    }
  }
}

object SlottedPipeBuilder {

  case class Factory(physicalPlan: PhysicalPlan)
    extends PipeBuilderFactory {
    override def apply(recurse: LogicalPlan => Pipe, readOnly: Boolean,
                       expressionConverters: ExpressionConverters)
                      (implicit context: PipeExecutionBuilderContext, tokenContext: TokenContext): PipeBuilder = {

      val expressionToExpression = recursePipes(recurse) _

      val fallback = InterpretedPipeBuilder(recurse, readOnly, expressionConverters, expressionToExpression, tokenContext)(context.semanticTable)

      new SlottedPipeBuilder(fallback, expressionConverters, physicalPlan, readOnly, expressionToExpression)
    }
  }

  private def projectSlotExpression(slot: Slot): commandExpressions.Expression = slot match {
    case LongSlot(offset, false, CTNode) =>
      slottedExpressions.NodeFromSlot(offset)
    case LongSlot(offset, true, CTNode) =>
      slottedExpressions.NullCheck(offset, slottedExpressions.NodeFromSlot(offset))
    case LongSlot(offset, false, CTRelationship) =>
      slottedExpressions.RelationshipFromSlot(offset)
    case LongSlot(offset, true, CTRelationship) =>
      slottedExpressions.NullCheck(offset, slottedExpressions.RelationshipFromSlot(offset))

    case RefSlot(offset, _, _) =>
      slottedExpressions.ReferenceFromSlot(offset)

    case _ =>
      throw new InternalException(s"Do not know how to project $slot")
  }

  type RowMapping = (ExecutionContext, QueryState) => ExecutionContext

  //compute mapping from incoming to outgoing pipe line, the slot order may differ
  //between the output and the input (lhs and rhs) and it may be the case that
  //we have a reference slot in the output but a long slot on one of the inputs,
  //e.g. MATCH (n) RETURN n UNION RETURN 42 AS n
  def computeUnionMapping(in: SlotConfiguration, out: SlotConfiguration): RowMapping = {
    val overlaps: Boolean = out.mapSlot {
      //For long slots we need to make sure both offset and types match
      //e.g we cannot allow mixing a node long slot with a relationship
      //longslot
      case (k, s: LongSlot) =>
        s == in.get(k).get
      //If both incoming and outgoing slots are refslot is is ok that types etc differs,
      // just make sure they have the same offset
      case (k, s: RefSlot) =>
        val inSlot = in.get(k).get
        inSlot.isInstanceOf[RefSlot] && s.offset == inSlot.offset

    }.forall(_ == true)

    //If we overlap we can just pass the result right through
    if (overlaps) {
      (incoming: ExecutionContext, _: QueryState) =>
        incoming
    }
    else {
      //find columns where output is a reference slot but where the input is a long slot

      val mapSlots: Iterable[(ExecutionContext, ExecutionContext, QueryState) => Unit] = out.mapSlot {
        case (k, v: LongSlot) =>
          val sourceOffset = in.getLongOffsetFor(k)
          (in, out, _) =>
            out.setLongAt(v.offset, in.getLongAt(sourceOffset))
        case (k, v: RefSlot) =>
          in.get(k).get match {
            case l: LongSlot => //here we must map the long slot to a reference slot
              val projectionExpression = projectSlotExpression(l) // Pre-compute projection expression
              (in, out, state) =>
                out.setRefAt(v.offset, projectionExpression(in, state))
            case _ =>
              val sourceOffset = in.getReferenceOffsetFor(k)
              (in, out, _) =>
                out.setRefAt(v.offset, in.getRefAt(sourceOffset))
          }
      }
      //Create a new context and apply all transformations
      (incoming: ExecutionContext, state: QueryState) =>
        val outgoing = SlottedExecutionContext(out)
        mapSlots.foreach(f => f(incoming, outgoing, state))
        outgoing
    }

  }

  def translateColumnOrder(slots: SlotConfiguration, s: plans.ColumnOrder): pipes.ColumnOrder = s match {
    case plans.Ascending(name) =>
      slots.get(name) match {
        case Some(slot) => pipes.Ascending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    case plans.Descending(name) =>
      slots.get(name) match {
        case Some(slot) => pipes.Descending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
  }
}

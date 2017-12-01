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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.frontend.v3_4.ast.ProcedureResultItem
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.{HasHeaders, IdName, NoHeaders, ShortestPathPattern}
import org.neo4j.cypher.internal.util.v3_4.{InternalException, UnNamedNameGenerator}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.{expressions => parserAst}

import scala.collection.mutable

/**
  * This object knows how to configure slots for a logical plan tree.
  *
  * The structure of the code is built this maybe weird way instead of being recursive to avoid the JVM execution stack
  * and instead handle the stacks manually here. Some queries we have seen are deep enough to crash the VM if not
  * configured carefully.
  *
  * The knowledge about how to actually allocate slots for each logical plan lives in the three `allocate` methods,
  * whereas the knowledge of how to traverse the plan tree is store in the while loops and stacks in the `populate`
  * method.
  **/
object SlotAllocation {

  private def NO_ARGUMENT() = SlotsAndArgument(SlotConfiguration.empty, Size.zero)

  case class SlotsAndArgument(slotConfiguration: SlotConfiguration, argumentSize: Size)

  // If we reintroduce pipeline again to hold the final slot size, which is needed when we create new execution context.
  // (actually leaf pipes should get new execution contexts from the pipeline, and terminator pipes should release them back to pipeline
  //  (which holds a pool of contexts))
  // We can then have an individual copy of SlotConfiguration for each plan id which holds the current slots and size that exists at that pipeline stage.
  // It can also hold liveness information (at least we can remove variables/symbols/aliases that are not live after a projection/aggregation (horizon))
  case class PhysicalPlan(slotConfigurations: Map[LogicalPlanId, SlotConfiguration],
                          argumentSizes: Map[LogicalPlanId, Size])

  /**
    * Allocate slot for every operator in the logical plan tree {@code lp}.
    *
    * @param lp the logical plan to process.
    * @return the slot configurations of every operator.
    */
  def allocateSlots(lp: LogicalPlan,
                    semanticTable: SemanticTable,
                    initialSlotsAndArgument: Option[SlotsAndArgument] = None): PhysicalPlan = {

    val allocations = new mutable.OpenHashMap[LogicalPlanId, SlotConfiguration]()
    val arguments = new mutable.OpenHashMap[LogicalPlanId, Size]()

    val planStack = new mutable.Stack[(Boolean, LogicalPlan)]()
    val resultStack = new mutable.Stack[SlotConfiguration]()
    val argumentStack = new mutable.Stack[SlotsAndArgument]()
    initialSlotsAndArgument.foreach(argumentStack.push)
    var comingFrom = lp

    def recordArgument(plan: LogicalPlan, argument: SlotsAndArgument) = {
      arguments += plan.assignedId -> argument.argumentSize
    }

    /**
      * Eagerly populate the stack using all the lhs children.
      */
    def populate(plan: LogicalPlan, nullIn: Boolean) = {
      var nullable = nullIn
      var current = plan
      while (!current.isLeaf) {
        if (current.isInstanceOf[Optional])
          nullable = true

        planStack.push((nullable, current))

        current = current.lhs.get // this should not fail unless we are on a leaf
      }
      comingFrom = current
      planStack.push((nullable, current))
    }

    populate(lp, nullIn = false)

    while (planStack.nonEmpty) {
      val (nullable, current) = planStack.pop()

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val argument = if (argumentStack.isEmpty) NO_ARGUMENT()
                         else argumentStack.top
          recordArgument(current, argument)
          val slotsIncludingExpressions = allocateExpressions(current, nullable, argument.slotConfiguration.copy(), allocations, arguments)(semanticTable)
          val result = allocate(current, nullable, slotsIncludingExpressions)
          allocations += (current.assignedId -> result)
          resultStack.push(result)

        case (Some(_), None) =>
          val sourceSlots = resultStack.pop()
          val argument = if (argumentStack.isEmpty) NO_ARGUMENT()
                         else argumentStack.top
          val slotsIncludingExpressions = allocateExpressions(current, nullable, sourceSlots, allocations, arguments)(semanticTable)
          val result = allocate(current, nullable, slotsIncludingExpressions, recordArgument(_, argument))
          allocations += (current.assignedId -> result)
          resultStack.push(result)

        case (Some(left), Some(right)) if (comingFrom eq left) && isAnApplyPlan(current) =>
          planStack.push((nullable, current))
          val argumentSlots = resultStack.top
          allocateLhsOfApply(current, nullable, argumentSlots)(semanticTable) // This never copies the slot configuration
          argumentStack.push(SlotsAndArgument(argumentSlots.copy(), argumentSlots.size()))
          populate(right, nullable)

        case (Some(left), Some(right)) if comingFrom eq left =>
          planStack.push((nullable, current))
          populate(right, nullable)

        case (Some(_), Some(right)) if comingFrom eq right =>
          val rhsSlots = resultStack.pop()
          val lhsSlots = resultStack.pop()
          val argument = if (argumentStack.isEmpty) NO_ARGUMENT()
                         else argumentStack.top
          // NOTE: If we introduce a two sourced logical plan with an expression that needs to be evaluated in a
          //       particular scope (lhs or rhs) we need to add handling of it to allocateExpressions.
          val lhsSlotsIncludingExpressions = allocateExpressions(current, nullable, lhsSlots, allocations, arguments, shouldAllocateLhs = true)(semanticTable)
          val rhsSlotsIncludingExpressions = allocateExpressions(current, nullable, rhsSlots, allocations, arguments, shouldAllocateLhs = false)(semanticTable)
          val result = allocate(current, nullable, lhsSlotsIncludingExpressions, rhsSlotsIncludingExpressions, recordArgument(_, argument))
          allocations += (current.assignedId -> result)
          if (isAnApplyPlan(current))
            argumentStack.pop()
          resultStack.push(result)
      }

      comingFrom = current
    }

    PhysicalPlan(allocations.toMap, arguments.toMap)
  }

  // NOTE: If we find a NestedPlanExpression within the given LogicalPlan, the slotConfigurations and argumentSizes maps will be updated
  private def allocateExpressions(lp: LogicalPlan, nullable: Boolean, slots: SlotConfiguration,
                                  slotConfigurations: mutable.Map[LogicalPlanId, SlotConfiguration],
                                  argumentSizes: mutable.Map[LogicalPlanId, Size],
                                  shouldAllocateLhs: Boolean = true)
                                 (semanticTable: SemanticTable): SlotConfiguration = {
    case class Accumulator(slots: SlotConfiguration, doNotTraverseExpression: Option[Expression])

    val TRAVERSE_INTO_CHILDREN = Some((s: Accumulator) => s)
    val DO_NOT_TRAVERSE_INTO_CHILDREN = None

    val result = lp.treeFold[Accumulator](Accumulator(slots, doNotTraverseExpression = None)) {
      //-----------------------------------------------------
      // Logical plans
      //-----------------------------------------------------
      case p: LogicalPlan if p.assignedId != lp.assignedId =>
        acc: Accumulator => (acc, DO_NOT_TRAVERSE_INTO_CHILDREN) // Do not traverse the logical plan tree! We are only looking at the given lp

      case ValueHashJoin(_, _, Equals(_, rhsExpression)) if shouldAllocateLhs =>
        acc: Accumulator =>
          (Accumulator(slots = acc.slots, doNotTraverseExpression = Some(rhsExpression)), TRAVERSE_INTO_CHILDREN) // Only look at lhsExpression

      case ValueHashJoin(_, _, Equals(lhsExpression, _)) if !shouldAllocateLhs =>
        acc: Accumulator =>
          (Accumulator(slots = acc.slots, doNotTraverseExpression = Some(lhsExpression)), TRAVERSE_INTO_CHILDREN) // Only look at rhsExpression

      // Only allocate expression on the LHS for these plans
      case _: AbstractSelectOrSemiApply | _: AbstractLetSelectOrSemiApply if !shouldAllocateLhs =>
        acc: Accumulator =>
          (acc, DO_NOT_TRAVERSE_INTO_CHILDREN)

      //-----------------------------------------------------
      // Expressions
      //-----------------------------------------------------
      case e: ScopeExpression =>
        acc: Accumulator => {
          if (acc.doNotTraverseExpression == e)
            (acc, DO_NOT_TRAVERSE_INTO_CHILDREN)
          else {
            e.introducedVariables.foreach {
              case v@Variable(name) =>
                slots.newReference(name, true, CTAny)
            }
            (acc, TRAVERSE_INTO_CHILDREN)
          }
        }

      case e: NestedPlanExpression =>
        acc: Accumulator => {
          if (acc.doNotTraverseExpression == e)
            (acc, DO_NOT_TRAVERSE_INTO_CHILDREN)
          else {
            val argumentSlotConfiguration = slots.copy()
            val slotsAndArgument = SlotsAndArgument(argumentSlotConfiguration, Size(slots.numberOfLongs, slots.numberOfReferences))

            // Allocate slots for nested plan
            val nestedPhysicalPlan = allocateSlots(e.plan, semanticTable, initialSlotsAndArgument = Some(slotsAndArgument))

            // Update the physical plan
            slotConfigurations ++= nestedPhysicalPlan.slotConfigurations
            argumentSizes ++= nestedPhysicalPlan.argumentSizes

            (acc, DO_NOT_TRAVERSE_INTO_CHILDREN) // We already recursively allocated the nested plan, so do not traverse into its children
          }
        }

      case e: Expression =>
        acc: Accumulator => {
          if (acc.doNotTraverseExpression == e)
            (acc, DO_NOT_TRAVERSE_INTO_CHILDREN)
          else
            (acc, TRAVERSE_INTO_CHILDREN)
        }
    }
    result.slots
  }

  /**
    * Compute the slot configuration of a leaf logical plan operator {@code lp}.
    *
    * @param lp the operator to compute slots for.
    * @param nullable
    * @param argument the logical plan argument slot configuration.
    * @return the slot configuration of lp
    */
  private def allocate(lp: LogicalPlan, nullable: Boolean, argument: SlotConfiguration): SlotConfiguration =
    lp match {
      case leaf: NodeLogicalLeafPlan =>
        val result = argument
        result.newLong(leaf.idName.name, nullable, CTNode)
        result

      case _:Argument =>
        argument

      case leaf: DirectedRelationshipByIdSeek =>
        val result = argument
        result.newLong(leaf.idName.name, nullable, CTRelationship)
        result.newLong(leaf.startNode.name, nullable, CTNode)
        result.newLong(leaf.endNode.name, nullable, CTNode)
        result

      case leaf: UndirectedRelationshipByIdSeek =>
        val result = argument
        result.newLong(leaf.idName.name, nullable, CTRelationship)
        result.newLong(leaf.leftNode.name, nullable, CTNode)
        result.newLong(leaf.rightNode.name, nullable, CTNode)
        result

      case leaf: NodeCountFromCountStore =>
        val result = argument
        result.newReference(leaf.idName.name, false, CTInteger)
        result

      case leaf: RelationshipCountFromCountStore =>
        val result = argument
        result.newReference(leaf.idName.name, false, CTInteger)
        result

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  /**
    * Compute the slot configuration of a single source logical plan operator {@code lp}.
    *
    * @param lp the operator to compute slots for.
    * @param nullable
    * @param source the slot configuration of the source operator.
    * @param recordArgument function which records the argument size for the given operator
    * @return the slot configuration of lp
    */
  private def allocate(lp: LogicalPlan,
                       nullable: Boolean,
                       source: SlotConfiguration,
                       recordArgument: LogicalPlan => Unit): SlotConfiguration =
    lp match {

      case Distinct(_, groupingExpressions) =>
        val result = SlotConfiguration.empty
        addGroupingMap(groupingExpressions, source, result)
        result

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val result = SlotConfiguration.empty
        addGroupingMap(groupingExpressions, source, result)

        aggregationExpressions foreach {
          case (key, _) =>
            result.newReference(key, nullable = true, CTAny)
        }
        result

      case Expand(_, _, _, _, IdName(to), IdName(relName), ExpandAll) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(relName, nullable, CTRelationship)
        result.newLong(to, nullable, CTNode)
        result

      case Expand(_, _, _, _, _, IdName(relName), ExpandInto) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(relName, nullable, CTRelationship)
        result

      case Optional(_, _) =>
        recordArgument(lp)
        source

      case _: ProduceResult |
           _: Selection |
           _: Limit |
           _: Skip |
           _: Sort |
           _: Top
      =>
        source

      case Projection(_, expressions) =>
        expressions foreach {
          case (key, parserAst.Variable(ident)) if key == ident =>
          // it's already there. no need to add a new slot for it

          case (newKey, parserAst.Variable(ident)) if newKey != ident =>
            source.addAlias(newKey, ident)

          case (key, _) =>
            source.newReference(key, nullable = true, CTAny)
        }
        source

      case OptionalExpand(_, _, _, _, IdName(to), IdName(rel), ExpandAll, _) =>
        // Note that OptionExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(rel, nullable = true, CTRelationship)
        result.newLong(to, nullable = true, CTNode)
        result

      case OptionalExpand(_, _, _, _, _, IdName(rel), ExpandInto, _) =>
        // Note that OptionExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(rel, nullable = true, CTRelationship)
        result

      case VarExpand(_,
                       IdName(from),
                       _,
                       _,
                       _,
                       IdName(to),
                       IdName(edge),
                       _,
                       expansionMode,
                       IdName(tempNode),
                       IdName(tempEdge),
                       _,
                       _,
                       _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()

        // We allocate these on the incoming pipeline after cloning it, since we don't need these slots in
        // the produced rows
        source.newLong(tempNode, nullable = false, CTNode)
        source.newLong(tempEdge, nullable = false, CTRelationship)

        if (expansionMode == ExpandAll) {
          result.newLong(to, nullable, CTNode)
        }
        result.newReference(edge, nullable, CTList(CTRelationship))
        result

      case PruningVarExpand(_, from, _, _, to, _, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(from.name, nullable, CTNode)
        result.newLong(to.name, nullable, CTNode)
        result

      case FullPruningVarExpand(_, from, _, _, to, _, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newLong(from.name, nullable, CTNode)
        result.newLong(to.name, nullable, CTNode)
        result

      case CreateNode(_, IdName(name), _, _) =>
        source.newLong(name, nullable = false, CTNode)
        source

      case _:MergeCreateNode =>
        // The variable name should already have been allocated by the NodeLeafPlan
        source

      case CreateRelationship(_, IdName(name), _, _, _, _) =>
        source.newLong(name, nullable = false, CTRelationship)
        source

      case MergeCreateRelationship(_, IdName(name), _, _, _, _) =>
        source.newLong(name, nullable = false, CTRelationship)
        source

      case EmptyResult(_) =>
        source

      case DropResult(_) =>
        source

      case UnwindCollection(_, IdName(variable), _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        val result = source.copy()
        result.newReference(variable, nullable = true, CTAny)
        result

      case Eager(_) =>
        source.copy()

      case _: DeleteNode |
           _: DeleteRelationship |
           _: DeletePath |
           _: DeleteExpression |
           _: DetachDeleteNode |
           _: DetachDeletePath |
           _: DetachDeleteExpression =>
        source

      case _: SetLabels |
           _: SetNodeProperty |
           _: SetNodePropertiesFromMap |
           _: SetRelationshipPropery |
           _: SetRelationshipPropertiesFromMap |
           _: SetProperty |
           _: RemoveLabels =>
        source

      case _: LockNodes =>
        source

      case ProjectEndpoints(_, _, start, startInScope, end, endInScope, _, _, _) =>
        if (!startInScope)
          source.newLong(start.name, nullable, CTNode)
        if (!endInScope)
          source.newLong(end.name, nullable, CTNode)
        source

      case LoadCSV(_, _, variableName, NoHeaders, _, _) =>
        source.newReference(variableName.name, nullable, CTList(CTAny))
        source

      case LoadCSV(_, _, variableName, HasHeaders, _, _) =>
        source.newReference(variableName.name, nullable, CTMap)
        source

      case ProcedureCall(_, ResolvedCall(_, _, callResults, _, _)) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        // Also, if the procedure is void it cannot increase cardinality.
        callResults.foreach {
          case ProcedureResultItem(output, variable) =>
            source.newReference(variable.name, true, CTAny)
        }
        source

      case FindShortestPaths(_, shortestPathPattern, predicates, withFallBack, disallowSameNode) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        allocateShortestPathPattern(shortestPathPattern, source, nullable)
        source

      case p =>
        throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  /**
    * Compute the slot configuration of a branching logical plan operator {@code lp}.
    *
    * @param lp the operator to compute slots for.
    * @param nullable
    * @param lhs the slot configuration of the left hand side operator.
    * @param rhs the slot configuration of the right hand side operator.
    * @return the slot configuration of lp
    */
  private def allocate(lp: LogicalPlan,
                       nullable: Boolean,
                       lhs: SlotConfiguration,
                       rhs: SlotConfiguration,
                       recordArgument: LogicalPlan => Unit): SlotConfiguration =
    lp match {
      case _: Apply =>
        rhs

      case _: AbstractSemiApply |
           _: AbstractSelectOrSemiApply =>
        lhs

      case _: AntiConditionalApply |
           _: ConditionalApply =>
        rhs

      case LetSemiApply(_, _, IdName(name)) =>
        lhs.newReference(name, false, CTBoolean)
        lhs

      case LetAntiSemiApply(_, _, IdName(name)) =>
        lhs.newReference(name, false, CTBoolean)
        lhs

      case LetSelectOrSemiApply(_, _, IdName(name), _) =>
        lhs.newReference(name, false, CTBoolean)
        lhs

      case LetSelectOrAntiSemiApply(_, _, IdName(name), _) =>
        lhs.newReference(name, false, CTBoolean)
        lhs

      case _: CartesianProduct =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = lhs.copy()
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
        rhs.foreachSlotOrdered {
          case (k, slot) =>
            result.add(k, slot)
        }
        result

      case NodeHashJoin(nodes, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val nodeKeys = nodes.map(_.name)
        val result = lhs.copy()
        rhs.foreachSlotOrdered {
          case (k, slot) if !nodeKeys(k) =>
            result.add(k, slot)
            // If the column is one of the join columns there is no need to add it again

          case _ =>
        }
        result

      case _: ValueHashJoin =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val slotConfig: SlotConfiguration = lhs.copy()
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
        rhs.foreachSlotOrdered {
          case (k, slot) =>
            slotConfig.add(k, slot)
        }
        slotConfig

      case OuterHashJoin(nodes, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val nodeKeys = nodes.map(_.name)
        val result = lhs.copy()
        rhs.foreachSlotOrdered {
          case (k, slot) if !nodeKeys(k) =>
            result.add(k, slot)
          // If the column is one of the join columns there is no need to add it again

          case _ =>
        }
        result

      case RollUpApply(_, _, collectionName, _, _) =>
        lhs.newReference(collectionName.name, nullable, CTList(CTAny))
        lhs

      case _: ForeachApply =>
        lhs

      case _: Union  =>
        // The result slot configuration should only contain the variables we join on.
        // If both lhs and rhs has a long slot with the same type the result should
        // also use a long slot, otherwise we use a ref slot.
        val result = SlotConfiguration.empty
        lhs.foreachSlot {
          case (key, lhsSlot: LongSlot) =>
            //find all shared variables and look for other long slots with same type
            rhs.get(key).foreach {
            case LongSlot(_, rhsNullable, typ) if typ == lhsSlot.typ =>
              result.newLong(key, lhsSlot.nullable || rhsNullable, typ)
            case rhsSlot =>
              val newType = if (lhsSlot.typ == rhsSlot.typ) lhsSlot.typ else CTAny
              result.newReference(key, lhsSlot.nullable || rhsSlot.nullable, newType)
            }
          case (key, lhsSlot) =>
            //We know lhs uses a ref slot so just look for shared variables.
            rhs.get(key).foreach {
              rhsSlot =>
                val newType = if (lhsSlot.typ == rhsSlot.typ) lhsSlot.typ else CTAny
                result.newReference(key, lhsSlot.nullable || rhsSlot.nullable, newType)
            }
        }
        result

      case _: AssertSameNode =>
        lhs

      case p =>
        throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  private def allocateLhsOfApply(plan: LogicalPlan,
                                 nullable: Boolean,
                                 lhs: SlotConfiguration)
                                (semanticTable: SemanticTable): SlotConfiguration =
    plan match {
      case ForeachApply(_, _, variableName, listExpression) =>
        // The slot for the iteration variable of foreach needs to be available as an argument on the rhs of the apply
        // so we allocate it on the lhs (even though its value will not be needed after the foreach is done)
        val typeSpec = semanticTable.getActualTypeFor(listExpression)
        if (typeSpec.contains(ListType(CTNode))) {
          lhs.newLong(variableName, true, CTNode)
        }
        else if (typeSpec.contains(ListType(CTRelationship))) {
          lhs.newLong(variableName, true, CTRelationship)
        }
        else {
          lhs.newReference(variableName, true, CTAny)
        }
        lhs

      case _ =>
        lhs
    }

  private def addGroupingMap(groupingExpressions: Map[String, Expression],
                             source: SlotConfiguration,
                             target: SlotConfiguration): Unit =
    groupingExpressions foreach {
      case (key, parserAst.Variable(ident)) =>
        val slotInfo = source(ident)
        target.newReference(key, slotInfo.nullable, slotInfo.typ)
      case (key, _) =>
        target.newReference(key, nullable = true, CTAny)
    }

  private def isAnApplyPlan(current: LogicalPlan): Boolean = current match {
    case _: AntiConditionalApply |
         _: Apply |
         _: AbstractSemiApply |
         _: AbstractSelectOrSemiApply |
         _: AbstractLetSelectOrSemiApply |
         _: AbstractLetSemiApply |
         _: ConditionalApply |
         _: ForeachApply |
         _: RollUpApply =>
      true

    case _ => false
  }

  private def allocateShortestPathPattern(shortestPathPattern: ShortestPathPattern,
                                          slots: SlotConfiguration,
                                          nullable: Boolean) = {
    val maybePathName = shortestPathPattern.name.map(_.name)
    val part = shortestPathPattern.expr
    val pathName = maybePathName.getOrElse(UnNamedNameGenerator.name(part.position))
    val rel = part.element match {
      case RelationshipChain(_, relationshipPattern, _) =>
        relationshipPattern
      case _ =>
        throw new IllegalStateException("This should be caught during semantic checking")
    }
    val relIteratorName = rel.variable.map(_.name)

    // Allocate slots
    slots.newReference(pathName, nullable, CTPath)
    if (relIteratorName.isDefined)
      slots.newReference(relIteratorName.get, nullable, CTList(CTRelationship))
  }
}

class SlotAllocationFailed(str: String) extends InternalException(str)

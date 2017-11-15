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
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.Expression
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

  case class PhysicalPlan(slotConfigurations: Map[LogicalPlanId, SlotConfiguration],
                          argumentSizes: Map[LogicalPlanId, Size])

  /**
    * Allocate slot for every operator in the logical plan tree {@code lp}.
    *
    * @param lp the logical plan to process.
    * @return the slot configurations of every operator.
    */
  def allocateSlots(lp: LogicalPlan): PhysicalPlan = {

    val allocations = new mutable.OpenHashMap[LogicalPlanId, SlotConfiguration]()
    val arguments = new mutable.OpenHashMap[LogicalPlanId, Size]()

    val planStack = new mutable.Stack[(Boolean, LogicalPlan)]()
    val resultStack = new mutable.Stack[SlotConfiguration]()
    val argumentStack = new mutable.Stack[SlotsAndArgument]()
    var comingFrom = lp

    def recordArgument(plan:LogicalPlan, argument: SlotsAndArgument) = {
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
          val result = allocate(current, nullable, argument.slotConfiguration)
          allocations += (current.assignedId -> result)
          resultStack.push(result)

        case (Some(_), None) =>
          val sourceSlots = resultStack.pop()
          val argument = if (argumentStack.isEmpty) NO_ARGUMENT()
                         else argumentStack.top
          val result = allocate(current, nullable, sourceSlots, recordArgument(_, argument))
          allocations += (current.assignedId -> result)
          resultStack.push(result)

        case (Some(left), Some(right)) if (comingFrom eq left) && isAnApplyPlan(current) =>
          planStack.push((nullable, current))
          val argumentSlots = resultStack.top
          argumentStack.push(SlotsAndArgument(argumentSlots.copy(), argumentSlots.size()))
          populate(right, nullable)

        case (Some(left), Some(right)) if comingFrom eq left =>
          planStack.push((nullable, current))
          populate(right, nullable)

        case (Some(_), Some(right)) if comingFrom eq right =>
          val rhsSlots = resultStack.pop()
          val lhsSlots = resultStack.pop()
          val result = allocate(current, nullable, lhsSlots, rhsSlots)
          allocations += (current.assignedId -> result)
          if (isAnApplyPlan(current))
            argumentStack.pop()
          resultStack.push(result)
      }

      comingFrom = current
    }

    PhysicalPlan(allocations.toMap, arguments.toMap)
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
        val result = source.copy()
        result.newLong(relName, nullable, CTRelationship)
        result.newLong(to, nullable, CTNode)
        result

      case Expand(_, _, _, _, _, IdName(relName), ExpandInto) =>
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

          case (key, parserAst.Variable(ident)) if key != ident =>
            val slot = source.get(ident).getOrElse(
              throw new SlotAllocationFailed(s"Tried to lookup key $key that should be in slot configuration but wasn't"))
            source.addAliasFor(slot, key)

          case (key, _) =>
            source.newReference(key, nullable = true, CTAny)
        }
        source

      case OptionalExpand(_, _, _, _, IdName(to), IdName(rel), ExpandAll, _) =>
        // Note that OptionExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
        val result = source.copy()
        result.newLong(rel, nullable = true, CTRelationship)
        result.newLong(to, nullable = true, CTNode)
        result

      case OptionalExpand(_, _, _, _, _, IdName(rel), ExpandInto, _) =>
        // Note that OptionExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
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
                       ExpandAll,
                       IdName(tempNode),
                       IdName(tempEdge),
                       _,
                       _,
                       _) =>
        val result = source.copy()

        // We allocate these on the incoming pipeline after cloning it, since we don't need these slots in
        // the produced rows
        source.newLong(tempNode, nullable = false, CTNode)
        source.newLong(tempEdge, nullable = false, CTRelationship)

        result.newLong(to, nullable, CTNode)
        result.newReference(edge, nullable, CTList(CTRelationship))
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
        val result = source.copy()
        result.newReference(variable, nullable = true, CTAny)
        result

      case Eager(_) =>
        source.copy()

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
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
                       rhs: SlotConfiguration): SlotConfiguration =
    lp match {
      case _: Apply =>
        rhs

      case _: SemiApply |
           _: AntiSemiApply =>
        lhs

      case _: AntiConditionalApply |
           _: ConditionalApply =>
        rhs

      case _: CartesianProduct =>
        val result = lhs.copy()
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
        rhs.foreachSlotOrdered {
          case (k, slot) =>
            result.add(k, slot)
        }
        result


      case NodeHashJoin(nodes, _, _) =>
        val nodeKeys = nodes.map(_.name)
        val result = lhs.copy()
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
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
      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
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
         _: ConditionalApply |
         _: ForeachApply |
         _: RollUpApply => true

    case _ => false
  }
}

class SlotAllocationFailed(str: String) extends InternalException(str)

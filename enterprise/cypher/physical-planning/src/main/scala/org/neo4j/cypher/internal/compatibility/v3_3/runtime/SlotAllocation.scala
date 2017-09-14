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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, ast => parserAst}
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.v3_3.logical.plans._

import scala.collection.mutable

/**
  * This object knows how to go from a query plan to pipelines with slot information calculated.
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

  def allocateSlots(lp: LogicalPlan): Map[LogicalPlanId, PipelineInformation] = {

    val result = new mutable.OpenHashMap[LogicalPlanId, PipelineInformation]()

    val planStack = new mutable.Stack[(Boolean, LogicalPlan)]()
    val outputStack = new mutable.Stack[PipelineInformation]()
    val argumentStack = new mutable.Stack[PipelineInformation]()
    var comingFrom = lp

    /**
      * eagerly populate the stack using all the lhs children
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
          val argument = if (argumentStack.isEmpty) None else Some(argumentStack.top)
          val output = allocate(current, nullable, argument)
          result += (current.assignedId -> output)
          outputStack.push(output)

        case (Some(_), None) =>
          val incomingPipeline = outputStack.pop()
          val output = allocate(current, nullable, incomingPipeline)
          result += (current.assignedId -> output)
          outputStack.push(output)

        case (Some(left), Some(right)) if (comingFrom eq left) && isAnApplyPlan(current) =>
          planStack.push((nullable, current))
          val argumentPipeline = outputStack.top
          argumentStack.push(argumentPipeline.seedClone())
          populate(right, nullable)

        case (Some(left), Some(right)) if comingFrom eq left =>
          planStack.push((nullable, current))
          populate(right, nullable)

        case (Some(_), Some(right)) if comingFrom eq right =>
          val rhsPipeline = outputStack.pop()
          val lhsPipeline = outputStack.pop()
          val output = allocate(current, nullable, lhsPipeline, rhsPipeline)
          result += (current.assignedId -> output)
          if (isAnApplyPlan(current))
            argumentStack.pop()
          outputStack.push(output)
      }

      comingFrom = current
    }

    result.toMap
  }

  private def allocate(lp: LogicalPlan, nullable: Boolean, argument: Option[PipelineInformation]): PipelineInformation =
    lp match {
      case _: Argument =>
        argument.getOrElse(throw new InternalException("Found argument without Apply"))

      case leaf: NodeLogicalLeafPlan =>
        val pipeline = argument.getOrElse(PipelineInformation.empty)
        pipeline.newLong(leaf.idName.name, nullable, CTNode)
        pipeline

      case SingleRow() =>
        argument.getOrElse(PipelineInformation.empty)

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  private def allocate(lp: LogicalPlan, nullable: Boolean, incomingPipeline: PipelineInformation): PipelineInformation =
    lp match {

      case Distinct(_, groupingExpressions) =>
        val outgoing = PipelineInformation.empty
        addGroupingMap(groupingExpressions, incomingPipeline, outgoing)
        outgoing

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val outgoing = PipelineInformation.empty
        addGroupingMap(groupingExpressions, incomingPipeline, outgoing)

        aggregationExpressions foreach {
          case (key, _) =>
            outgoing.newReference(key, nullable = true, CTAny)
        }
        outgoing

      case Expand(_, _, _, _, IdName(to), IdName(relName), ExpandAll) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline.newLong(relName, nullable, CTRelationship)
        newPipeline.newLong(to, nullable, CTNode)
        newPipeline

      case Expand(_, _, _, _, _, IdName(relName), ExpandInto) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline.newLong(relName, nullable, CTRelationship)
        newPipeline

      case Optional(_, _) =>
        incomingPipeline

      case _: ProduceResult |
           _: Selection |
           _: Limit |
           _: Skip |
           _: Sort |
           _: Top
      =>
        incomingPipeline

      case Projection(_, expressions) =>
        expressions foreach {
          case (key, parserAst.Variable(ident)) if key == ident =>
          // it's already there. no need to add a new slot for it
          case (key, _) =>
            incomingPipeline.newReference(key, nullable = true, CTAny)
        }
        incomingPipeline

      case OptionalExpand(_, _, _, _, IdName(to), IdName(rel), ExpandAll, _) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline.newLong(rel, nullable = true, CTRelationship)
        newPipeline.newLong(to, nullable = true, CTNode)
        newPipeline

      case OptionalExpand(_, _, _, _, _, IdName(rel), ExpandInto, _) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline.newLong(rel, nullable = true, CTRelationship)
        newPipeline

      case VarExpand(lhs: LogicalPlan,
                       IdName(from),
                       dir,
                       projectedDir,
                       types,
                       IdName(to),
                       IdName(edge),
                       length,
                       ExpandAll,
                       IdName(tempNode),
                       IdName(tempEdge),
                       _,
                       _,
                       _) =>
        val newPipeline = incomingPipeline.seedClone()

        // We allocate these on the incoming pipeline after cloning it, since we don't need these slots in
        // the produced rows
        incomingPipeline.newLong(tempNode, nullable = false, CTNode)
        incomingPipeline.newLong(tempEdge, nullable = false, CTRelationship)

        newPipeline.newLong(to, nullable, CTNode)
        newPipeline.newReference(edge, nullable, CTList(CTRelationship))
        newPipeline

      case CreateNode(_, IdName(name), _, _) =>
        incomingPipeline.newLong(name, nullable = false, CTNode)
        incomingPipeline

      case MergeCreateNode(_, IdName(name), _, _) =>
        // The variable name should already have been allocated by the NodeLeafPlan
        incomingPipeline

      case CreateRelationship(_, IdName(name), startNode, typ, endNode, props) =>
        incomingPipeline.newLong(name, nullable = false, CTRelationship)
        incomingPipeline

      case MergeCreateRelationship(_, IdName(name), startNode, typ, endNode, props) =>
        incomingPipeline.newLong(name, nullable = false, CTRelationship)
        incomingPipeline

      case EmptyResult(_) =>
        incomingPipeline

      case UnwindCollection(_, IdName(variable), expression) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline.newReference(variable, nullable = true, CTAny)
        newPipeline

      case Eager(_) =>
        val newPipeline = incomingPipeline.seedClone()
        newPipeline

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  private def addGroupingMap(groupingExpressions: Map[String, Expression], incoming: PipelineInformation, outgoing: PipelineInformation) = {
    groupingExpressions foreach {
      case (key, parserAst.Variable(ident)) =>
        val slotInfo = incoming(ident)
        outgoing.newReference(key, slotInfo.nullable, slotInfo.typ)
      case (key, _) =>
        outgoing.newReference(key, nullable = true, CTAny)
    }
  }

  private def allocate(plan: LogicalPlan,
                       nullable: Boolean,
                       lhsPipeline: PipelineInformation,
                       rhsPipeline: PipelineInformation): PipelineInformation =
    plan match {
      case _: Apply =>
        rhsPipeline

      case _: SemiApply |
           _: AntiSemiApply =>
        lhsPipeline

      case _: AntiConditionalApply |
           _: ConditionalApply =>
        rhsPipeline

      case _:CartesianProduct =>
        val cartesianProductPipeline = lhsPipeline.seedClone()
        rhsPipeline.foreachSlot {
          case (k, slot) =>
            cartesianProductPipeline.add(k, slot)
        }
        cartesianProductPipeline

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
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

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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.planner.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, Rewriter, topDown}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, LogicalPlanId, NestedPlanExpression, Projection, VarExpand, _}
import org.neo4j.cypher.internal.v3_4.{functions => frontendFunctions}

import scala.collection.mutable

/**
  * This class rewrites logical plans so they use slotted variable access instead of using key-based. It will also
  * rewrite the slot configurations so that the new plans can be found in there.
  *
  * // TODO: Not too sure about that rewrite comment. Revisit here when cleaning up rewriting.
  *
  * @param tokenContext the token context used to map between token ids and names.
  */
class SlottedRewriter(tokenContext: TokenContext) {

  private def rewriteUsingIncoming(oldPlan: LogicalPlan): Boolean = oldPlan match {
    case _: Aggregation | _: Distinct => true
    case _ => false
  }

  def apply(in: LogicalPlan, pipelineInformation: Map[LogicalPlanId, SlotConfiguration]): LogicalPlan = {
    val newPipelineInfo = mutable.HashMap[LogicalPlan, SlotConfiguration]()
    val rewritePlanWithSlots = topDown(Rewriter.lift {
      /*
      Projection means executing expressions and writing the result to a row. Since any expression of Variable-type
      would just write to the row the data that is already in it, we can just skip them
       */
      case oldPlan@Projection(_, expressions) =>
        val information = pipelineInformation(oldPlan.assignedId)
        val rewriter = rewriteCreator(information, oldPlan)

        val newExpressions = expressions collect {
          case (column, expression) => column -> expression.endoRewrite(rewriter)
        }

        val newPlan = oldPlan.copy(expressions = newExpressions)(oldPlan.solved)
        newPipelineInfo += (newPlan -> information)

        newPlan

      case oldPlan: VarExpand =>
        /*
        The node and edge predicates will be set and evaluated on the incoming rows, not on the outgoing ones.
        We need to use the incoming pipeline info for predicate rewriting
         */
        val incomingPipeline = pipelineInformation(oldPlan.source.assignedId)
        val rewriter = rewriteCreator(incomingPipeline, oldPlan)

        val newNodePredicate = oldPlan.nodePredicate.endoRewrite(rewriter)
        val newEdgePredicate = oldPlan.edgePredicate.endoRewrite(rewriter)

        val newPlan = oldPlan.copy(
          nodePredicate = newNodePredicate,
          edgePredicate = newEdgePredicate,
          legacyPredicates = Seq.empty // If we use the legacy predicates, we are not on the slotted runtime
        )(oldPlan.solved)

        /*
        Since the logical plan PipeInformation is about the output rows we still need to remember the
        outgoing pipeline info here
         */
        val outgoingPipeline = pipelineInformation(oldPlan.assignedId)
        newPipelineInfo += (newPlan -> outgoingPipeline)

        newPlan

      case oldPlan: LogicalPlan if rewriteUsingIncoming(oldPlan) =>
        val leftPlan = oldPlan.lhs.getOrElse(throw new InternalException("Leaf plans cannot be rewritten this way"))
        val incomingPipeline = pipelineInformation(leftPlan.assignedId)
        val rewriter = rewriteCreator(incomingPipeline, oldPlan)
        val newPlan = oldPlan.endoRewrite(rewriter)

        /*
        Since the logical plan PipeInformation is about the output rows we still need to remember the
        outgoing pipeline info here
         */
        val outgoingPipeline = pipelineInformation(oldPlan.assignedId)
        newPipelineInfo += (newPlan -> outgoingPipeline)
        newPlan

      case oldPlan: LogicalPlan =>
        val information = pipelineInformation(oldPlan.assignedId)
        val rewriter = rewriteCreator(information, oldPlan)
        val newPlan = oldPlan.endoRewrite(rewriter)
        newPipelineInfo += (newPlan -> information)

        newPlan
    })

    // Rewrite plan and note which logical plans are rewritten to something else
    val resultPlan = in.endoRewrite(rewritePlanWithSlots)

    // TODO: This should probably only run when -ea is enabled
    resultPlan.findByAllClass[Variable].foreach(v => throw new CantCompileQueryException(s"Failed to rewrite away $v\n$resultPlan"))

    resultPlan
  }

  private def rewriteCreator(pipelineInformation: SlotConfiguration, thisPlan: LogicalPlan): Rewriter = {
    val innerRewriter = Rewriter.lift {
      case prop@Property(Variable(key), PropertyKeyName(propKey)) =>

        pipelineInformation(key) match {
          case LongSlot(offset, nullable, typ) =>
            val maybeToken: Option[Int] = tokenContext.getOptPropertyKeyId(propKey)

            val propExpression = (typ, maybeToken) match {
              case (CTNode, Some(token)) => NodeProperty(offset, token, s"$key.$propKey")
              case (CTNode, None) => NodePropertyLate(offset, propKey, s"$key.$propKey")
              case (CTRelationship, Some(token)) => RelationshipProperty(offset, token, s"$key.$propKey")
              case (CTRelationship, None) => RelationshipPropertyLate(offset, propKey, s"$key.$propKey")
              case _ => throw new InternalException(s"Expressions on object other then nodes and relationships are not yet supported")
            }
            if (nullable)
              NullCheck(offset, propExpression)
            else
              propExpression

          case RefSlot(offset, _, _) => prop.copy(map = ReferenceFromSlot(offset))(prop.position)
        }

      case e@Equals(Variable(k1), Variable(k2)) => // TODO: Handle nullability
        val slot1 = pipelineInformation(k1)
        val slot2 = pipelineInformation(k2)
        if (slot1.typ == slot2.typ && SlotConfiguration.isLongSlot(slot1) && SlotConfiguration.isLongSlot(slot2)) {
          PrimitiveEquals(IdFromSlot(slot1.offset), IdFromSlot(slot2.offset))
        }
        else
          e

      case GetDegree(Variable(n), typ, direction) =>
        val maybeToken: Option[String] = typ.map(r => r.name)
        pipelineInformation(n) match {
          case LongSlot(offset, false, CTNode) => GetDegreePrimitive(offset, maybeToken, direction)
          case LongSlot(offset, true, CTNode) => NullCheck(offset, GetDegreePrimitive(offset, maybeToken, direction))
          case _ => throw new CantCompileQueryException(s"Invalid slot for GetDegree: $n")
        }

      case Variable(k) =>
        pipelineInformation.get(k) match {
          case Some(slot) => slot match {
            case LongSlot(offset, false, CTNode) => NodeFromSlot(offset, k)
            case LongSlot(offset, true, CTNode) => NullCheck(offset, NodeFromSlot(offset, k))
            case LongSlot(offset, false, CTRelationship) => RelationshipFromSlot(offset, k)
            case LongSlot(offset, true, CTRelationship) => NullCheck(offset, RelationshipFromSlot(offset, k))
            case RefSlot(offset, _, _) => ReferenceFromSlot(offset)
            case _ =>
              throw new CantCompileQueryException("Unknown type for `" + k + "` in the pipeline information")
          }
          case _ =>
            throw new CantCompileQueryException("Did not find `" + k + "` in the pipeline information")
        }

      case idFunction: FunctionInvocation if idFunction.function == frontendFunctions.Id =>
        idFunction.args.head match {
          case Variable(key) =>
            val slot = pipelineInformation(key)
            slot match {
              case LongSlot(offset, true, _) => NullCheck(offset, IdFromSlot(offset))
              case LongSlot(offset, false, _) => IdFromSlot(offset)
              case _ => idFunction // Don't know how to specialize this
            }
          case _ => idFunction // Don't know how to specialize this
        }

      case idFunction: FunctionInvocation if idFunction.function == frontendFunctions.Exists =>
        idFunction.args.head match {
          case Property(Variable(key), PropertyKeyName(propKey)) =>
            checkIfPropertyExists(pipelineInformation, key, propKey)
          case _ => idFunction // Don't know how to specialize this
        }

      case e@IsNull(Property(Variable(key), PropertyKeyName(propKey))) =>
        Not(checkIfPropertyExists(pipelineInformation, key, propKey))(e.position)

      case _: ShortestPathExpression =>
        throw new CantCompileQueryException(s"Expressions with shortestPath functions not yet supported in slot allocation")

      case _: ScopeExpression | _: NestedPlanExpression =>
        throw new CantCompileQueryException(s"Expressions with inner scope are not yet supported in slot allocation")

      case _: PatternExpression =>
        throw new CantCompileQueryException(s"Pattern expressions not yet supported in the slotted runtime")
    }
    topDown(rewriter = innerRewriter, stopper = stopAtOtherLogicalPlans(thisPlan))
  }

  private def checkIfPropertyExists(pipelineInformation: SlotConfiguration, key: String, propKey: String) = {
    val slot = pipelineInformation(key)
    val maybeToken = tokenContext.getOptPropertyKeyId(propKey)

    val propExpression = (slot, maybeToken) match {
      case (LongSlot(offset, _, typ), Some(token)) if typ == CTNode =>
        NodePropertyExists(offset, token, s"$key.$propKey")

      case (LongSlot(offset, _, typ), None) if typ == CTNode =>
        NodePropertyExistsLate(offset, propKey, s"$key.$propKey")

      case (LongSlot(offset, _, typ), Some(token)) if typ == CTRelationship =>
        RelationshipPropertyExists(offset, token, s"$key.$propKey")

      case (LongSlot(offset, _, typ), None) if typ == CTRelationship =>
        RelationshipPropertyExistsLate(offset, propKey, s"$key.$propKey")

      case _ => throw new CantCompileQueryException(s"Expressions on object other then nodes and relationships are not yet supported")
    }

    if (slot.nullable)
      NullCheck(slot.offset, propExpression)
    else
      propExpression
  }

  private def stopAtOtherLogicalPlans(thisPlan: LogicalPlan): (AnyRef) => Boolean = {
    lp => lp.isInstanceOf[LogicalPlan] && lp != thisPlan
  }
}

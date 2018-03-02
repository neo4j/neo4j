/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.planner.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.util.v3_4.AssertionUtils.ifAssertionsEnabled
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.attribution.SameId
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, Rewriter, topDown}
import org.neo4j.cypher.internal.v3_4.expressions.{FunctionInvocation, _}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NestedPlanExpression, Projection, VarExpand, _}
import org.neo4j.cypher.internal.v3_4.{expressions, functions => frontendFunctions}

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

  def apply(in: LogicalPlan, slotConfigurations: SlotConfigurations): LogicalPlan = {
    val rewritePlanWithSlots = topDown(Rewriter.lift {
      /*
      Projection means executing expressions and writing the result to a row. Since any expression of Variable-type
      would just write to the row the data that is already in it, we can just skip them
       */
      case oldPlan@Projection(_, expressions) =>
        val slotConfiguration = slotConfigurations(oldPlan.id)
        val rewriter = rewriteCreator(slotConfiguration, oldPlan.selfThis, slotConfigurations)

        val newExpressions = expressions collect {
          case (column, expression) => column -> expression.endoRewrite(rewriter)
        }

        val newPlan = oldPlan.copy(expressions = newExpressions)(SameId(oldPlan.id))

        newPlan

      case oldPlan: VarExpand =>
        /*
        The node and edge predicates will be set and evaluated on the incoming rows, not on the outgoing ones.
        We need to use the incoming slot configuration for predicate rewriting
         */
        val incomingSlotConfiguration = slotConfigurations(oldPlan.source.id)
        val rewriter = rewriteCreator(incomingSlotConfiguration, oldPlan, slotConfigurations)

        val newNodePredicate = oldPlan.nodePredicate.endoRewrite(rewriter)
        val newEdgePredicate = oldPlan.edgePredicate.endoRewrite(rewriter)

        val newPlan = oldPlan.copy(
          nodePredicate = newNodePredicate,
          edgePredicate = newEdgePredicate,
          legacyPredicates = Seq.empty // If we use the legacy predicates, we are not on the slotted runtime
        )(SameId(oldPlan.id))

        /*
        Since the logical plan SlotConfiguration is about the output rows we still need to remember the
        outgoing slot configuration here
         */
        val outgoingSlotConfiguration = slotConfigurations(oldPlan.id)

        newPlan

      case plan@ValueHashJoin(lhs, rhs, e@Equals(lhsExp, rhsExp)) =>
        val lhsRewriter = rewriteCreator(slotConfigurations(lhs.id), plan.selfThis, slotConfigurations)
        val rhsRewriter = rewriteCreator(slotConfigurations(rhs.id), plan.selfThis, slotConfigurations)
        val lhsExpAfterRewrite = lhsExp.endoRewrite(lhsRewriter)
        val rhsExpAfterRewrite = rhsExp.endoRewrite(rhsRewriter)
        plan.copy(join = Equals(lhsExpAfterRewrite, rhsExpAfterRewrite)(e.position))(SameId(plan.id))

      case oldPlan: LogicalPlan if rewriteUsingIncoming(oldPlan) =>
        val leftPlan = oldPlan.lhs.getOrElse(throw new InternalException("Leaf plans cannot be rewritten this way"))
        val incomingSlotConfiguration = slotConfigurations(leftPlan.id)
        val rewriter = rewriteCreator(incomingSlotConfiguration, oldPlan, slotConfigurations)
        val newPlan = oldPlan.endoRewrite(rewriter)

        /*
        Since the logical plan SlotConfiguration is about the output rows we still need to remember the
        outgoing slot configuration here
         */
        val outgoingSlotConfiguration = slotConfigurations(oldPlan.id)
        newPlan

      case oldPlan: LogicalPlan =>
        val slotConfiguration = slotConfigurations(oldPlan.id)
        val rewriter = rewriteCreator(slotConfiguration, oldPlan, slotConfigurations)
        val newPlan = oldPlan.endoRewrite(rewriter)

        newPlan
    })

    // Rewrite plan and note which logical plans are rewritten to something else
    val resultPlan = in.endoRewrite(rewritePlanWithSlots)

    // Verify that we could rewrite all instances of Variable (only under -ea)
    ifAssertionsEnabled {
      resultPlan.findByAllClass[Variable].foreach(v => throw new CantCompileQueryException(s"Failed to rewrite away $v\n$resultPlan"))
    }

    resultPlan
  }

  private def rewriteCreator(slotConfiguration: SlotConfiguration, thisPlan: LogicalPlan, slotConfigurations: SlotConfigurations): Rewriter = {
    val innerRewriter = Rewriter.lift {
      case e: NestedPlanExpression =>
        // Rewrite expressions within the nested plan
        val rewrittenPlan = this.apply(e.plan, slotConfigurations)
        val innerSlotConf = slotConfigurations.getOrElse(e.plan.id,
          throw new InternalException(s"Missing slot configuration for plan with ${e.plan.id}"))
        val rewriter = rewriteCreator(innerSlotConf, thisPlan, slotConfigurations)
        val rewrittenProjection = e.projection.endoRewrite(rewriter)
        e.copy(plan = rewrittenPlan, projection = rewrittenProjection)(e.position)

      case prop@Property(Variable(key), PropertyKeyName(propKey)) =>

        slotConfiguration(key) match {
          case LongSlot(offset, nullable, typ) =>
            val maybeToken: Option[Int] = tokenContext.getOptPropertyKeyId(propKey)

            val propExpression = (typ, maybeToken) match {
              case (CTNode, Some(token)) => NodeProperty(offset, token, s"$key.$propKey")(prop)
              case (CTNode, None) => NodePropertyLate(offset, propKey, s"$key.$propKey")(prop)
              case (CTRelationship, Some(token)) => RelationshipProperty(offset, token, s"$key.$propKey")(prop)
              case (CTRelationship, None) => RelationshipPropertyLate(offset, propKey, s"$key.$propKey")(prop)
              case _ => throw new InternalException(s"Expressions on object other then nodes and relationships are not yet supported")
            }
            if (nullable)
              NullCheckProperty(offset, propExpression)
            else
              propExpression

          case RefSlot(offset, _, _) =>
            prop.copy(map = ReferenceFromSlot(offset, key))(prop.position)
        }

      case e@Equals(Variable(k1), Variable(k2)) =>
        primitiveEqualityChecks(slotConfiguration, e, k1, k2, positiveCheck = true)

      case Not(e@Equals(Variable(k1), Variable(k2))) =>
        primitiveEqualityChecks(slotConfiguration, e, k1, k2, positiveCheck = false)

      case e@IsNull(Variable(key)) =>
        val slot = slotConfiguration(key)
        slot match {
          case LongSlot(offset, true, _) => IsPrimitiveNull(offset)
          case LongSlot(_, false, _) => False()(e.position)
          case _ => e
        }

      case GetDegree(Variable(n), typ, direction) =>
        val maybeToken: Option[String] = typ.map(r => r.name)
        slotConfiguration(n) match {
          case LongSlot(offset, false, CTNode) => GetDegreePrimitive(offset, maybeToken, direction)
          case LongSlot(offset, true, CTNode) => NullCheck(offset, GetDegreePrimitive(offset, maybeToken, direction))
          case _ => throw new CantCompileQueryException(s"Invalid slot for GetDegree: $n")
        }

      case v @ Variable(k) =>
        slotConfiguration.get(k) match {
          case Some(slot) => slot match {
            case LongSlot(offset, false, CTNode) => NodeFromSlot(offset, k)
            case LongSlot(offset, true, CTNode) => NullCheckVariable(offset, NodeFromSlot(offset, k))
            case LongSlot(offset, false, CTRelationship) => RelationshipFromSlot(offset, k)
            case LongSlot(offset, true, CTRelationship) => NullCheckVariable(offset, RelationshipFromSlot(offset, k))
            case RefSlot(offset, _, _) => ReferenceFromSlot(offset, k)
            case _ =>
              throw new CantCompileQueryException("Unknown type for `" + k + "` in the slot configuration")
          }
          case _ =>
            throw new CantCompileQueryException("Did not find `" + k + "` in the slot configuration")
        }

      case idFunction: FunctionInvocation if idFunction.function == frontendFunctions.Id =>
        idFunction.args.head match {
          case Variable(key) =>
            val slot = slotConfiguration(key)
            slot match {
              case LongSlot(offset, true, _) => NullCheck(offset, IdFromSlot(offset))
              case LongSlot(offset, false, _) => IdFromSlot(offset)
              case _ => idFunction // Don't know how to specialize this
            }
          case _ => idFunction // Don't know how to specialize this
        }

      case existsFunction: FunctionInvocation if existsFunction.function == frontendFunctions.Exists =>
        existsFunction.args.head match {
          case prop @ Property(Variable(key), PropertyKeyName(propKey)) =>
            val maybeSpecializedExpression = specializeCheckIfPropertyExists(slotConfiguration, key, propKey, prop)
            maybeSpecializedExpression.getOrElse(existsFunction)

          case _ => existsFunction // Don't know how to specialize this
        }

      case e @ IsNull(prop @ Property(Variable(key), PropertyKeyName(propKey))) =>
        val maybeSpecializedExpression = specializeCheckIfPropertyExists(slotConfiguration, key, propKey, prop)
        if (maybeSpecializedExpression.isDefined)
          Not(maybeSpecializedExpression.get)(e.position)
        else
          e

//      case _: ReduceExpression =>
//        throw new CantCompileQueryException(s"Expressions with reduce are not yet supported in slot allocation")
//
//      case _: DesugaredMapProjection =>
//        throw new CantCompileQueryException(s"Expressions with map projections are not yet supported in slot allocation")
//
//      case _: ShortestPathExpression =>
//        throw new CantCompileQueryException(s"Expressions with shortestPath functions not yet supported in slot allocation")
//
//      case _: PatternExpression =>
//        throw new CantCompileQueryException(s"Pattern expressions not yet supported in the slotted runtime")
    }
    topDown(rewriter = innerRewriter, stopper = stopAtOtherLogicalPlans(thisPlan))
  }

  private def primitiveEqualityChecks(slots: SlotConfiguration,
                                      e: Equals,
                                      k1: String,
                                      k2: String,
                                      positiveCheck: Boolean) = {
    def makeNegativeIfNeeded(e: expressions.Expression) = if (!positiveCheck)
      Not(e)(e.position)
    else
      e

    val shortcutWhenDifferentTypes: expressions.Expression = if(positiveCheck) False()(e.position) else True()(e.position)
    val slot1 = slots(k1)
    val slot2 = slots(k2)

    (slot1, slot2) match {
      // If we are trying to compare two different types, we'll never return true.
      // But if we are comparing nullable things, we need to do extra null checks before returning false.
      // this case only handles the situation where it's safe to straight away rewrite to false, e.g;
      // MATCH (n)-[r]->()
      // WHERE n = r
      case (LongSlot(_, false, typ1), LongSlot(_, false, typ2)) if typ1 != typ2 =>
        shortcutWhenDifferentTypes

      case (LongSlot(_, false, typ1), LongSlot(_, false, typ2)) if typ1 == typ2 =>
        val eq = PrimitiveEquals(IdFromSlot(slot1.offset), IdFromSlot(slot2.offset))
        makeNegativeIfNeeded(eq)

      case (LongSlot(_, null1, typ1), LongSlot(_, null2, typ2))
        if (null1 || null2) && (typ1 != typ2) =>
        makeNullChecksExplicit(slot1, slot2, shortcutWhenDifferentTypes)

      case (LongSlot(_, null1, typ1), LongSlot(_, null2, typ2))
        if (null1 || null2) && (typ1 == typ2) =>
        val eq = PrimitiveEquals(IdFromSlot(slot1.offset), IdFromSlot(slot2.offset))
        makeNullChecksExplicit(slot1, slot2, makeNegativeIfNeeded(eq))

      case _ =>
        makeNegativeIfNeeded(e)
    }
  }

  private def makeNullChecksExplicit(slot1: Slot, slot2: Slot, predicate: expressions.Expression) = {
    // If a slot is nullable, we rewrite the equality to make null handling explicit and not part of the equality check:
    // <nullableLhs> <predicate> <rhs> ==>
    // NOT(<nullableLhs> IS NULL) AND <nullableLhs> <predicate> <rhs>
    def nullCheckIfNeeded(slot: Slot, p: expressions.Expression): expressions.Expression =
      if (slot.nullable)
        NullCheck(slot.offset, p)
      else
        p

    nullCheckIfNeeded(slot1,
      nullCheckIfNeeded(slot2,
        predicate))
  }

  private def specializeCheckIfPropertyExists(slotConfiguration: SlotConfiguration, key: String, propKey: String, prop: Property) = {
    val slot = slotConfiguration(key)
    val maybeToken = tokenContext.getOptPropertyKeyId(propKey)

    val propExpression = (slot, maybeToken) match {
      case (LongSlot(offset, _, typ), Some(token)) if typ == CTNode =>
        Some(NodePropertyExists(offset, token, s"$key.$propKey")(prop))

      case (LongSlot(offset, _, typ), None) if typ == CTNode =>
        Some(NodePropertyExistsLate(offset, propKey, s"$key.$propKey")(prop))

      case (LongSlot(offset, _, typ), Some(token)) if typ == CTRelationship =>
        Some(RelationshipPropertyExists(offset, token, s"$key.$propKey")(prop))

      case (LongSlot(offset, _, typ), None) if typ == CTRelationship =>
        Some(RelationshipPropertyExistsLate(offset, propKey, s"$key.$propKey")(prop))

      case _ =>
        None // Let the normal expression conversion work this out
    }

    if (slot.nullable && propExpression.isDefined && propExpression.get.isInstanceOf[LogicalProperty]) {
      Some(NullCheckProperty(slot.offset, propExpression.get.asInstanceOf[LogicalProperty]))
    }
    else
      propExpression
  }

  private def stopAtOtherLogicalPlans(thisPlan: LogicalPlan): (AnyRef) => Boolean = {
    case lp@(_: LogicalPlan) =>
      lp.id != thisPlan.id

    // Do not traverse into slotted runtime variables or properties
    case _: RuntimeVariable | _: RuntimeProperty =>
      true

    case _ =>
      false
  }
}

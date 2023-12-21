/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.AbstractVarExpand
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.TrailPlans
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter.rewriteVariable
import org.neo4j.cypher.internal.physicalplanning.ast.ElementIdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.GetDegreePrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasALabelFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasAnyLabelFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeGreaterThanOrEqualPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeGreaterThanPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeLessThanOrEqualPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeLessThanPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreePrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasLabelsFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasTypesFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.IdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.IsPrimitiveNull
import org.neo4j.cypher.internal.physicalplanning.ast.LabelsFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.MapProjectionFromStore
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyExists
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyExistsLate
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheck
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckReferenceProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckVariable
import org.neo4j.cypher.internal.physicalplanning.ast.PrimitiveEquals
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipProperty
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyExists
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyExistsLate
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipTypeFromSlot
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.ast.RuntimeProperty
import org.neo4j.cypher.internal.runtime.ast.RuntimeVariable
import org.neo4j.cypher.internal.runtime.ast.VariableRef
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException

/**
 * This class rewrites logical plans so they use slotted variable access instead of using key-based.
 *
 * @param tokenContext the token context used to map between token ids and names.
 */
class SlottedRewriter(tokenContext: ReadTokenContext) {

  def apply(in: LogicalPlan, slotConfigurations: SlotConfigurations, trailPlans: TrailPlans): LogicalPlan = {

    val rewritePlanWithSlots =
      topDown(Rewriter.lift {
        case oldPlan: AbstractVarExpand =>
          /*
        The node and edge predicates will be set and evaluated on the incoming rows, not on the outgoing ones.
        We need to use the incoming slot configuration for predicate rewriting
           */
          val incomingSlotConfiguration = slotConfigurations(oldPlan.source.id)
          val incomingRewriter = rewriteCreator(incomingSlotConfiguration, oldPlan, slotConfigurations, trailPlans)

          val newNodePredicates =
            oldPlan.nodePredicates.map(x => VariablePredicate(x.variable, x.predicate.endoRewrite(incomingRewriter)))
          val newRelationshipPredicates =
            oldPlan.relationshipPredicates.map(x =>
              VariablePredicate(x.variable, x.predicate.endoRewrite(incomingRewriter))
            )

          val oldPlanWithNewPredicates =
            oldPlan.withNewPredicates(newNodePredicates, newRelationshipPredicates)(SameId(oldPlan.id))

          val slotConfiguration = slotConfigurations(oldPlan.id)
          val rewriter = rewriteCreator(slotConfiguration, oldPlan, slotConfigurations, trailPlans)
          val newPlan = oldPlanWithNewPredicates.endoRewrite(rewriter)

          newPlan

        case plan @ ValueHashJoin(lhs, rhs, e @ Equals(lhsExp, rhsExp)) =>
          val lhsRewriter = rewriteCreator(slotConfigurations(lhs.id), plan, slotConfigurations, trailPlans)
          val rhsRewriter = rewriteCreator(slotConfigurations(rhs.id), plan, slotConfigurations, trailPlans)
          val lhsExpAfterRewrite = lhsExp.endoRewrite(lhsRewriter)
          val rhsExpAfterRewrite = rhsExp.endoRewrite(rhsRewriter)
          plan.copy(join = Equals(lhsExpAfterRewrite, rhsExpAfterRewrite)(e.position))(SameId(plan.id))

        case oldPlan: AggregatingPlan =>
          // Grouping and aggregation expressions needs to be rewritten using the incoming slot configuration
          // as these slots are not available on the outgoing rows
          val leftPlan = oldPlan.lhs.getOrElse(throw new InternalException("Leaf plans cannot be rewritten this way"))
          val slotConfiguration = slotConfigurations(oldPlan.id)
          val incomingSlotConfiguration = slotConfigurations(leftPlan.id)
          val incomingRewriter = rewriteCreator(incomingSlotConfiguration, oldPlan, slotConfigurations, trailPlans)
          val newGroupingExpressions = oldPlan.groupingExpressions collect {
            case (column, expression) =>
              rewriteVariable(oldPlan, column, slotConfiguration) -> expression.endoRewrite(incomingRewriter)
          }
          val newAggregationExpressions = oldPlan.aggregationExpressions collect {
            case (column, expression) =>
              rewriteVariable(oldPlan, column, slotConfiguration) -> expression.endoRewrite(incomingRewriter)
          }
          val newOrderToLeverage = oldPlan.orderToLeverage collect {
            case expression => expression.endoRewrite(incomingRewriter)
          }
          val newPlan = oldPlan.withNewExpressions(
            newGroupingExpressions,
            newAggregationExpressions,
            newOrderToLeverage
          )(SameId(oldPlan.id))
          newPlan

        case plan @ RollUpApply(lhs, rhs, collectionName, variableToCollect) =>
          val lhsSlotConfiguration = slotConfigurations(lhs.id)
          val rhsSlotConfiguration = slotConfigurations(rhs.id)
          val newCollectionName = rewriteVariable(plan, collectionName, lhsSlotConfiguration)
          val newVariableToCollect = rewriteVariable(plan, variableToCollect, rhsSlotConfiguration)

          plan.copy(collectionName = newCollectionName, variableToCollect = newVariableToCollect)(SameId(plan.id))

        case oldPlan: LogicalPlan =>
          val slotConfiguration = slotConfigurations(oldPlan.id)
          val rewriter = rewriteCreator(slotConfiguration, oldPlan, slotConfigurations, trailPlans)
          val newPlan = oldPlan.endoRewrite(rewriter)

          newPlan
      })

    // Rewrite plan and note which logical plans are rewritten to something else
    val resultPlan = in
      .endoRewrite(rewritePlanWithSlots)
      .endoRewrite(PostSlottedRewriter)

    checkOnlyWhenAssertionsAreEnabled(!resultPlan.folder.findAllByClass[Variable].exists(v =>
      throw new CantCompileQueryException(s"Failed to rewrite away $v\n$resultPlan")
    ))

    resultPlan
  }

  private def rewriteCreator(
    slotConfiguration: SlotConfiguration,
    thisPlan: LogicalPlan,
    slotConfigurations: SlotConfigurations,
    trailPlans: TrailPlans
  ): Rewriter = {
    val innerRewriter = Rewriter.lift {
      case e: NestedPlanExpression =>
        // Rewrite expressions within the nested plan
        val rewrittenPlan = this.apply(e.plan, slotConfigurations, trailPlans)
        val innerSlotConf = slotConfigurations.getOrElse(
          e.plan.id,
          throw new InternalException(s"Missing slot configuration for plan with ${e.plan.id}")
        )
        val rewriter = rewriteCreator(innerSlotConf, thisPlan, slotConfigurations, trailPlans)
        e match {
          case ce @ NestedPlanCollectExpression(_, projection, _) =>
            val rewrittenProjection = projection.endoRewrite(rewriter)
            ce.copy(plan = rewrittenPlan, projection = rewrittenProjection)(e.position)
          case ee: NestedPlanExistsExpression =>
            ee.copy(plan = rewrittenPlan)(e.position)
          case ee: NestedPlanGetByNameExpression =>
            ee.copy(plan = rewrittenPlan)(e.position)
        }

      case prop @ Property(Variable(key), PropertyKeyName(propKey)) =>
        slotConfiguration(key) match {
          case LongSlot(offset, nullable, typ) =>
            val maybeToken: Option[Int] = tokenContext.getOptPropertyKeyId(propKey)

            val propExpression = (typ, maybeToken) match {
              case (CTNode, Some(token))         => NodeProperty(offset, token, s"$key.$propKey")(prop)
              case (CTNode, None)                => NodePropertyLate(offset, propKey, s"$key.$propKey")(prop)
              case (CTRelationship, Some(token)) => RelationshipProperty(offset, token, s"$key.$propKey")(prop)
              case (CTRelationship, None)        => RelationshipPropertyLate(offset, propKey, s"$key.$propKey")(prop)
              case _ => throw new InternalException(
                  s"Expressions on object other then nodes and relationships are not yet supported"
                )
            }
            if (nullable)
              NullCheckProperty(offset, propExpression)
            else
              propExpression

          // The map-expression is always checked for NO_VALUE at runtime by the Property command expression,
          // which is why we do not need an explicit null-check here when the slot is nullable
          case RefSlot(offset, _, _) =>
            prop.copy(map = ReferenceFromSlot(offset, key))(prop.position)
        }

      case prop: CachedHasProperty =>
        rewriteCachedProperies(slotConfiguration, prop, needsValue = false)

      case prop: CachedProperty =>
        rewriteCachedProperies(slotConfiguration, prop, needsValue = true)

      case e @ Equals(Variable(k1), Variable(k2)) =>
        primitiveEqualityChecks(slotConfiguration, e, k1, k2, positiveCheck = true)

      case Not(e @ Equals(Variable(k1), Variable(k2))) =>
        primitiveEqualityChecks(slotConfiguration, e, k1, k2, positiveCheck = false)

      case e @ IsNull(Variable(key)) =>
        val slot = slotConfiguration(key)
        slot match {
          case LongSlot(offset, true, _) => IsPrimitiveNull(offset)
          case LongSlot(_, false, _)     => False()(e.position)
          case _                         => e
        }

      case original @ GetDegree(Variable(n), typ, direction) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) => GetDegreePrimitive(offset, maybeToken, direction)
          // For ref-slots, we just use the non-specialized GetDegree
          case _ => original
        }

      case original @ HasDegreeGreaterThan(Variable(n), typ, direction, degree) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) => HasDegreeGreaterThanPrimitive(offset, maybeToken, direction, degree)
          // For ref-slots, we just use the non-specialized HasDegreeGreaterThan
          case _ => original
        }

      case original @ HasDegreeGreaterThanOrEqual(Variable(n), typ, direction, degree) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) =>
            HasDegreeGreaterThanOrEqualPrimitive(offset, maybeToken, direction, degree)
          // For ref-slots, we just use the non-specialized HasDegreeGreaterThanOrEqual
          case _ => original
        }

      case original @ HasDegree(Variable(n), typ, direction, degree) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) => HasDegreePrimitive(offset, maybeToken, direction, degree)
          // For ref-slots, we just use the non-specialized HasDegree
          case _ => original
        }

      case original @ HasDegreeLessThan(Variable(n), typ, direction, degree) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) => HasDegreeLessThanPrimitive(offset, maybeToken, direction, degree)
          // For ref-slots, we just use the non-specialized HasDegreeLessThan
          case _ => original
        }

      case original @ HasDegreeLessThanOrEqual(Variable(n), typ, direction, degree) =>
        val maybeToken: Option[Either[Int, String]] = typ.map(r => tokenContext.getOptRelTypeId(r.name).toLeft(r.name))
        slotConfiguration(n) match {
          case LongSlot(offset, _, CTNode) =>
            HasDegreeLessThanOrEqualPrimitive(offset, maybeToken, direction, degree)
          // For ref-slots, we just use the non-specialized HasDegreeLessThanOrEqual
          case _ => original
        }

      case v: Variable =>
        rewriteVariable(thisPlan, v, slotConfiguration)

      case idFunction: FunctionInvocation if idFunction.function == expressions.functions.Id =>
        idFunction.args.head match {
          case Variable(key) =>
            val slot = slotConfiguration(key)
            slot match {
              case LongSlot(offset, true, _)  => NullCheck(offset, IdFromSlot(offset))
              case LongSlot(offset, false, _) => IdFromSlot(offset)
              case _                          => idFunction // Don't know how to specialize this
            }
          case _ => idFunction // Don't know how to specialize this
        }

      case idFunction: FunctionInvocation if idFunction.function == expressions.functions.ElementId =>
        idFunction.args.head match {
          case Variable(key) => slotConfiguration(key) match {
              case ElementIdFromSlot(elementIdFromSlot) => elementIdFromSlot
              case _                                    => idFunction // Don't know how to specialize this
            }
          case _ => idFunction // Don't know how to specialize this
        }

      case existsFunction: FunctionInvocation if existsFunction.function == expressions.functions.Exists =>
        existsFunction // Don't know how to specialize this

      case labels: FunctionInvocation if labels.function == expressions.functions.Labels =>
        labels.args.head match {
          case Variable(key) =>
            val slot = slotConfiguration(key)
            slot match {
              case LongSlot(offset, true, CTNode)  => NullCheck(offset, LabelsFromSlot(offset))
              case LongSlot(offset, false, CTNode) => LabelsFromSlot(offset)
              case _                               => labels // Don't know how to specialize this
            }
          case _ => labels // Don't know how to specialize this
        }

      case relType: FunctionInvocation if relType.function == expressions.functions.Type =>
        relType.args.head match {
          case Variable(key) =>
            val slot = slotConfiguration(key)
            slot match {
              case LongSlot(offset, true, CTRelationship)  => NullCheck(offset, RelationshipTypeFromSlot(offset))
              case LongSlot(offset, false, CTRelationship) => RelationshipTypeFromSlot(offset)
              case _                                       => relType // Don't know how to specialize this
            }
          case _ => relType // Don't know how to specialize this
        }

      case count: FunctionInvocation if count.function == expressions.functions.Count =>
        // Specialize counting of primitive nodes/relationships to avoid unnecessarily creating NodeValue/RelationshipValue objects
        count.args.head match {
          case v @ Variable(key) =>
            val slot = slotConfiguration(key)
            val maybeNewInnerExpression =
              if (count.distinct) {
                // If using DISTINCT we need the id value. Use IdFromSlot()
                slot match {
                  case LongSlot(offset, true, CTNode)          => Some(NullCheck(offset, IdFromSlot(offset)))
                  case LongSlot(offset, false, CTNode)         => Some(IdFromSlot(offset))
                  case LongSlot(offset, true, CTRelationship)  => Some(NullCheck(offset, IdFromSlot(offset)))
                  case LongSlot(offset, false, CTRelationship) => Some(IdFromSlot(offset))
                  case _                                       => None // Don't know how to specialize this
                }
              } else {
                // Else if not using DISTINCT, the Count() function only cares if the value != Values.NO_VALUE, so we just use a static Literal expression in place of the entity
                slot match {
                  case LongSlot(offset, true, CTNode) => Some(NullCheck(offset, True()(v.position)))
                  case LongSlot(_, false, CTNode) =>
                    Some(True()(v.position)) // Can never be null so we do not even have to check the slot
                  case LongSlot(offset, true, CTRelationship) => Some(NullCheck(offset, True()(v.position)))
                  case LongSlot(_, false, CTRelationship) =>
                    Some(True()(v.position)) // Can never be null so we do not even have to check the slot
                  case _ => None // Don't know how to specialize this
                }
              }
            if (maybeNewInnerExpression.isDefined) {
              count.copy(args = IndexedSeq(maybeNewInnerExpression.get))(count.position)
            } else {
              count
            }
          case _ => count // Don't know how to specialize this
        }

      case e @ HasLabels(Variable(k), labels) =>
        slotConfiguration(k) match {
          case LongSlot(offset, false, CTNode) =>
            val (resolvedLabelTokens, lateLabels) = resolveLabelTokens(labels)
            HasLabelsFromSlot(offset, resolvedLabelTokens, lateLabels)

          case LongSlot(offset, true, CTNode) =>
            val (resolvedLabelTokens, lateLabels) = resolveLabelTokens(labels)
            NullCheck(offset, HasLabelsFromSlot(offset, resolvedLabelTokens, lateLabels))

          case _ => e // Don't know how to specialize this
        }

      case e @ HasAnyLabel(Variable(k), labels) =>
        slotConfiguration(k) match {
          case LongSlot(offset, nullable, CTNode) =>
            val (resolvedLabelTokens, lateLabels) = resolveLabelTokens(labels)
            val hasAnyLabel = HasAnyLabelFromSlot(offset, resolvedLabelTokens, lateLabels)
            if (nullable) NullCheck(offset, hasAnyLabel) else hasAnyLabel

          case _ => e // Don't know how to specialize this
        }

      case e @ HasTypes(Variable(k), types) =>
        slotConfiguration(k) match {
          case LongSlot(offset, false, CTRelationship) =>
            val (resolvedTypeTokens, lateTypes) = resolveTypeTokens(types)
            HasTypesFromSlot(offset, resolvedTypeTokens, lateTypes)

          case LongSlot(offset, true, CTRelationship) =>
            val (resolvedTypeTokens, lateTypes) = resolveTypeTokens(types)
            NullCheck(offset, HasTypesFromSlot(offset, resolvedTypeTokens, lateTypes))

          case _ => e // Don't know how to specialize this
        }

      case e @ HasALabelOrType(Variable(k)) =>
        slotConfiguration(k) match {
          case LongSlot(offset, false, CTNode) =>
            HasALabelFromSlot(offset)

          case LongSlot(offset, true, CTNode) =>
            NullCheck(offset, HasALabelFromSlot(offset))

          case LongSlot(_, _, CTRelationship) =>
            True()(e.position)

          case _ => e // Don't know how to specialize this
        }

      case e @ HasLabelsOrTypes(Variable(k), labelsOrTypes) =>
        slotConfiguration(k) match {
          case LongSlot(offset, false, CTNode) =>
            val (resolvedLabelTokens, lateLabels) = resolveLabelTokens(labelsOrTypes)
            HasLabelsFromSlot(offset, resolvedLabelTokens, lateLabels)

          case LongSlot(offset, true, CTNode) =>
            val (resolvedLabelTokens, lateLabels) = resolveLabelTokens(labelsOrTypes)
            NullCheck(offset, HasLabelsFromSlot(offset, resolvedLabelTokens, lateLabels))

          case LongSlot(offset, false, CTRelationship) =>
            val (resolvedTypeTokens, lateTypes) =
              resolveTypeTokens(labelsOrTypes.map(t => RelTypeName(t.name)(t.position)))
            HasTypesFromSlot(offset, resolvedTypeTokens, lateTypes)

          case LongSlot(offset, true, CTRelationship) =>
            val (resolvedTypeTokens, lateTypes) =
              resolveTypeTokens(labelsOrTypes.map(t => RelTypeName(t.name)(t.position)))
            NullCheck(offset, HasTypesFromSlot(offset, resolvedTypeTokens, lateTypes))

          case _ => e // Don't know how to specialize this
        }

      case e @ IsNull(prop @ Property(Variable(key), PropertyKeyName(propKey))) =>
        val slot = slotConfiguration(key)
        val maybeSpecializedExpression = specializeCheckIfPropertyExists(slotConfiguration, key, propKey, prop, slot)
        if (maybeSpecializedExpression.isDefined) {
          val propertyExists = maybeSpecializedExpression.get
          val notPropertyExists = Not(propertyExists)(e.position)
          if (slot.nullable)
            Or(IsPrimitiveNull(slot.offset), notPropertyExists)(e.position)
          else
            notPropertyExists
        } else
          e

      case e @ IsNotNull(prop @ Property(Variable(key), PropertyKeyName(propKey))) =>
        val slot = slotConfiguration(key)
        val maybeSpecializedExpression = specializeCheckIfPropertyExists(slotConfiguration, key, propKey, prop, slot)
        if (maybeSpecializedExpression.isDefined) {
          val propertyExists = maybeSpecializedExpression.get
          if (slot.nullable)
            And(Not(IsPrimitiveNull(slot.offset))(e.position), propertyExists)(e.position)
          else
            propertyExists
        } else
          e

      case IsRepeatTrailUnique(innerRel: Variable) =>
        val trailId: Id = trailPlans.getOrElse(
          thisPlan.id,
          throw new InternalException("Expected IsRepeatTrailUnique to be under Trail")
        )
        ast.TrailRelationshipUniqueness(SlotAllocation.TRAIL_STATE_METADATA_KEY, trailId.x, innerRel)

      // Inside an NFA there are cases where we need to use a VariableRef
      case state @ NFA.State(_, Variable(name)) =>
        state.copy(variable = VariableRef(name))

      case re: NFA.RelationshipExpansionPredicate =>
        re.copy(relationshipVariable = VariableRef(re.relationshipVariable))
    }
    topDown(rewriter = innerRewriter, stopper = stopAtOtherLogicalPlans(thisPlan))
  }

  private def rewriteCachedProperies(
    slotConfiguration: SlotConfiguration,
    prop: ASTCachedProperty,
    needsValue: Boolean
  ) = {
    val pkn = prop.propertyKey
    val PropertyKeyName(propKey) = pkn
    val entityType = prop.entityType
    val originalEntityName = prop.originalEntityName
    slotConfiguration(prop.entityName) match {
      case LongSlot(offset, nullable, cypherType)
        if (cypherType == CTNode && entityType == NODE_TYPE) || (cypherType == CTRelationship && entityType == RELATIONSHIP_TYPE) =>
        val propExpression = tokenContext.getOptPropertyKeyId(propKey) match {
          case Some(propId) =>
            ast.SlottedCachedPropertyWithPropertyToken(
              originalEntityName,
              pkn,
              offset,
              offsetIsForLongSlot = true,
              propId,
              slotConfiguration.getCachedPropertyOffsetFor(prop),
              entityType,
              nullable,
              needsValue
            )
          case None =>
            ast.SlottedCachedPropertyWithoutPropertyToken(
              originalEntityName,
              pkn,
              offset,
              offsetIsForLongSlot = true,
              propKey,
              slotConfiguration.getCachedPropertyOffsetFor(prop),
              entityType,
              nullable,
              needsValue
            )
        }
        // Primitive entities are always null-checked by the CachedNodeProperty command expression itself at runtime,
        // which is why we do not need an explicit null-check here when the slot is nullable
        propExpression

      case slot @ LongSlot(_, _, _) =>
        throw new InternalException(s"Unexpected type on slot '$slot' for cached property $prop")

      // We can skip checking the type of the refslot. We will only get cached properties, if semantic analysis determined that an expression is
      // a node or a relationship. We loose this information for RefSlots for some expressions, otherwise we would have allocated long slots
      // in the first place.
      case RefSlot(offset, nullable, _) =>
        val propExpression = tokenContext.getOptPropertyKeyId(propKey) match {
          case Some(propId) =>
            ast.SlottedCachedPropertyWithPropertyToken(
              originalEntityName,
              pkn,
              offset,
              offsetIsForLongSlot = false,
              propId,
              slotConfiguration.getCachedPropertyOffsetFor(prop),
              entityType,
              nullable,
              needsValue
            )
          case None =>
            ast.SlottedCachedPropertyWithoutPropertyToken(
              originalEntityName,
              pkn,
              offset,
              offsetIsForLongSlot = false,
              propKey,
              slotConfiguration.getCachedPropertyOffsetFor(prop),
              entityType,
              nullable,
              needsValue
            )
        }
        if (nullable)
          NullCheckReferenceProperty(offset, propExpression)
        else
          propExpression
    }
  }

  private def resolveLabelTokens(labels: Seq[SymbolicName]): (Seq[Int], Seq[String]) = {
    val maybeTokens = labels.map(l => (tokenContext.getOptLabelId(l.name), l.name))
    val (resolvedLabelTokens, lateLabels) = maybeTokens.partition(_._1.isDefined)
    (resolvedLabelTokens.flatMap(_._1), lateLabels.map(_._2))
  }

  def resolveTypeTokens(types: Seq[SymbolicName]): (Seq[Int], Seq[String]) = {
    val maybeTokens = types.map(l => (tokenContext.getOptRelTypeId(l.name), l.name))
    val (resolvedTypeTokens, lateTypes) = maybeTokens.partition(_._1.isDefined)
    (resolvedTypeTokens.flatMap(_._1), lateTypes.map(_._2))
  }

  private def primitiveEqualityChecks(
    slots: SlotConfiguration,
    e: Equals,
    k1: String,
    k2: String,
    positiveCheck: Boolean
  ) = {
    def makeNegativeIfNeeded(e: expressions.Expression) =
      if (!positiveCheck)
        Not(e)(e.position)
      else
        e

    val shortcutWhenDifferentTypes: expressions.Expression =
      if (positiveCheck) False()(e.position) else True()(e.position)
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

      case (LongSlot(_, null1, typ1), LongSlot(_, null2, typ2)) if (null1 || null2) && (typ1 != typ2) =>
        makeNullChecksExplicit(slot1, slot2, shortcutWhenDifferentTypes)

      case (LongSlot(_, null1, typ1), LongSlot(_, null2, typ2)) if (null1 || null2) && (typ1 == typ2) =>
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

    nullCheckIfNeeded(slot1, nullCheckIfNeeded(slot2, predicate))
  }

  private def specializeCheckIfPropertyExists(
    slotConfiguration: SlotConfiguration,
    key: String,
    propKey: String,
    prop: Property,
    slot: Slot
  ) = {
    val maybeToken = tokenContext.getOptPropertyKeyId(propKey)

    (slot, maybeToken) match {
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
  }

  private def stopAtOtherLogicalPlans(thisPlan: LogicalPlan): RewriterStopper = {
    case lp @ (_: LogicalPlan) =>
      lp.id != thisPlan.id

    // Do not traverse into slotted runtime variables or properties
    case _: RuntimeVariable | _: RuntimeProperty =>
      true

    // NestedPlanGetByNameExpression can't be rewritten because slot is not known
    case _: NestedPlanGetByNameExpression => true

    case _ =>
      false
  }
}

object SlottedRewriter {

  /**
   * Most expressions that are specialized by the SlottedRewriter only give benefits
   * with long slots, so an expression containing only an offset without specification
   * on whether it's for the ref or long slots, the offset should be interpreted as being
   * a long slot offset.
   */
  val DEFAULT_OFFSET_IS_FOR_LONG_SLOT = true

  /**
   * Most specializations in the SlottedRewriter deal with slot nullability by wrapping
   * the slot expression in a NullCheck(..), meaning that the specialized expression
   * itself can assume the slot to not be nullable.
   */
  val DEFAULT_NULLABLE = false

  def rewriteVariable(
    thisPlan: LogicalPlan,
    v: LogicalVariable,
    slotConfiguration: SlotConfiguration
  ): LogicalVariable = {
    v match {
      case Variable(k) =>
        slotConfiguration.get(k) match {
          case Some(slot) => slot match {
              case LongSlot(offset, false, CTNode)         => NodeFromSlot(offset, k)
              case LongSlot(offset, true, CTNode)          => NullCheckVariable(offset, NodeFromSlot(offset, k))
              case LongSlot(offset, false, CTRelationship) => RelationshipFromSlot(offset, k)
              case LongSlot(offset, true, CTRelationship)  => NullCheckVariable(offset, RelationshipFromSlot(offset, k))
              case RefSlot(offset, _, _)                   => ReferenceFromSlot(offset, k)
              case _ =>
                throw new CantCompileQueryException("Unknown type for `" + k + "` in the slot configuration")
            }
          case _ =>
            throw new CantCompileQueryException(
              s"Did not find `$k` in the slot configuration of ${thisPlan.getClass.getSimpleName} (${thisPlan.id})"
            )
        }
      case _ =>
        throw new CantCompileQueryException(
          s"Don't know how to rewrite variable $v in ${thisPlan.getClass.getSimpleName} (${thisPlan.id})"
        )
    }
  }
}

/**
 * Rewrites that are independent from and needs to come after SlottedRewriter
 */
object PostSlottedRewriter extends Rewriter {

  private val instance = bottomUp {
    Rewriter.lift {
      case MapProjectionFromStore(projection) => projection
    }
  }

  override def apply(v: AnyRef): AnyRef = instance.apply(v)
}

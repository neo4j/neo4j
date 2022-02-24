/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.AbstractLetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.InternalException

import scala.collection.mutable

case object PushdownPropertyReads {

  // Negligible quantity of cardinality when considering pushdown
  private val CARDINALITY_EPSILON = EffectiveCardinality(0.0000001)

  /**
   * Rewrites the specified plan to include CacheProperties at cardinality optimums.
   *
   * Note, input position is NOT guaranteed to be accurate in cached properties.
   */
  def pushdown(logicalPlan: LogicalPlan,
               effectiveCardinalities: EffectiveCardinalities,
               attributes: Attributes[LogicalPlan],
               semanticTable: SemanticTable): LogicalPlan = {

    def isNodeOrRel(variable: LogicalVariable): Boolean =
      semanticTable.types.get(variable)
        .exists(t => t.actual == CTNode.invariant || t.actual == CTRelationship.invariant)

    case class CardinalityOptimum(cardinality: EffectiveCardinality, logicalPlanId: Id, variableName: String)
    case class Acc(variableOptima: Map[String, CardinalityOptimum],
                   propertyReadOptima: Seq[(CardinalityOptimum, Property)],
                   availableProperties: Set[Property],
                   availableWholeEntities: Set[String],
                   incomingCardinality: EffectiveCardinality)

    def foldSingleChildPlan(acc: Acc, plan: LogicalPlan): Acc = {
      val newPropertyExpressions =
        plan.treeFold(List.empty[Property]) {
          case lp: LogicalPlan if lp.id != plan.id =>
            acc2 => SkipChildren(acc2) // do not traverse further
          case _: CaseExpression => // we don't want to pushdown properties inside case expressions since it's not sure we will ever need to read them
            acc2 => SkipChildren(acc2)
          case p @ Property(v: LogicalVariable, _) if isNodeOrRel(v) =>
            acc2 => TraverseChildren(p :: acc2)
        }

      val newPushableProperties: Map[LogicalVariable, List[(CardinalityOptimum, Property)]] =
        newPropertyExpressions.flatMap {
          case p @ Property(v: LogicalVariable, _) =>
            acc.variableOptima.get(v.name) match {
              case Some(optimum: CardinalityOptimum) =>
                if (!acc.availableProperties.contains(p) && !acc.availableWholeEntities.contains(v.name))
                  Some((optimum, p))
                else
                  None
              // this happens for variables introduced in expressions, we ignore those for now
              case None => None
            }

          case e => throw new IllegalStateException(s"$e is not a valid property expression")
        }.groupBy { case (_, Property(v: LogicalVariable, _)) => v }

      val newPropertyReadOptima =
        newPushableProperties.toSeq.flatMap {
          case (v, optimumProperties) =>
            val (optimum, _) = optimumProperties.head
            if (optimum.cardinality < acc.incomingCardinality) {
              optimumProperties
            } else if (optimumProperties.size > 1 && plan.rhs.isEmpty && plan.lhs.nonEmpty && !plan.isInstanceOf[Selection]) {
              val uniqueProps = optimumProperties.map(_._2).toSet
              if (uniqueProps.size > 1) {
                val beforeThisPlanOptimum = CardinalityOptimum(acc.incomingCardinality, plan.lhs.get.id, v.name)
                uniqueProps.toSeq.map(prop => (beforeThisPlanOptimum, prop))
              } else {
                Nil
              }
            } else {
              Nil
            }
        }

      val outgoingCardinality = effectiveCardinalities(plan.id)
      val outgoingReadOptima = acc.propertyReadOptima ++ newPropertyReadOptima

      plan match {
        case _: Anti =>
          throw new IllegalStateException("This plan is introduced in physical planning, I shouldn't need to know about it.")

        case _: Aggregation |
             _: OrderedAggregation |
             _: Eager =>
          // Do _not_ pushdown past these plans
          val newVariables = plan.availableSymbols
          val outgoingVariableOptima = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v))).toMap

          Acc(outgoingVariableOptima, outgoingReadOptima, Set.empty, Set.empty, outgoingCardinality)

        case p: ProjectingPlan => // except for aggregations which were already matched
          val renamings: Map[String, String] =
            p.projectExpressions.collect {
              case (key, v: Variable) if key != v.name => (v.name, key)
            }

          val renamedVariableOptima =
            acc.variableOptima.map {
              case (oldName, optimum) =>
                (renamings.getOrElse(oldName, oldName), optimum)
            }

          val renamedAvailableProperties =
            (acc.availableProperties ++ newPropertyExpressions).map(
              prop => {
                val propVariable = prop.map.asInstanceOf[LogicalVariable].name
                renamings.get(propVariable) match {
                  case Some(newName) => propertyWithName(newName, prop)
                  case None => prop
                }
              })

          Acc(renamedVariableOptima, outgoingReadOptima, renamedAvailableProperties, acc.availableWholeEntities, outgoingCardinality)

        case _ =>
          val newLowestCardinalities =
            acc.variableOptima.view.mapValues(optimum =>
              if (outgoingCardinality <= (optimum.cardinality + CARDINALITY_EPSILON)) {
                CardinalityOptimum(outgoingCardinality, plan.id, optimum.variableName)
              } else {
                optimum
              }
            )

          val currentVariables = plan.availableSymbols
          val newVariables = currentVariables -- acc.variableOptima.keySet
          val newVariableCardinalities = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v)))
          val outgoingVariableOptima = newLowestCardinalities ++ newVariableCardinalities

          val propertiesFromPlan: Seq[Property] =
            plan match {
              case indexPlan: NodeIndexLeafPlan =>
                indexPlan.properties
                  .filter(_.getValueFromIndex == CanGetValue)
                  // NOTE: as we pushdown before inserting cached properties
                  //       the getValue behaviour will still be CanGetValue
                  //       instead of GetValue
                  .map(asProperty(indexPlan.idName))
              case indexPlan: RelationshipIndexLeafPlan =>
                indexPlan.properties
                  .filter(_.getValueFromIndex == CanGetValue)
                  // NOTE: as we pushdown before inserting cached properties
                  //       the getValue behaviour will still be CanGetValue
                  //       instead of GetValue
                  .map(asProperty(indexPlan.idName))

              case SetProperty(_, variable: LogicalVariable, propertyKey, _) =>
                Seq(Property(variable, propertyKey)(InputPosition.NONE))

              case SetProperties(_, variable: LogicalVariable, items) =>
                items.map {
                  case (p, _) => Property(variable, p)(InputPosition.NONE)
                }

              case SetNodeProperty(_, idName, propertyKey, _) =>
                Seq(Property(Variable(idName)(InputPosition.NONE), propertyKey)(InputPosition.NONE))

              case SetNodeProperties(_, idName, items) =>
                items.map {
                  case (p, _) => Property(Variable(idName)(InputPosition.NONE), p)(InputPosition.NONE)
                }

              case SetRelationshipProperty(_, idName, propertyKey, _) =>
                Seq(Property(Variable(idName)(InputPosition.NONE), propertyKey)(InputPosition.NONE))

              case SetRelationshipProperties(_, idName, items) =>
                items.map {
                  case (p, _) => Property(Variable(idName)(InputPosition.NONE), p)(InputPosition.NONE)
                }

              case SetNodePropertiesFromMap(_, idName, map: MapExpression, false) =>
                propertiesFromMap(idName, map)

              case SetRelationshipPropertiesFromMap(_, idName, map: MapExpression, false) =>
                propertiesFromMap(idName, map)

              case _ => Seq.empty
            }

          val maybeEntityFromPlan =
            plan match {
              case SetNodePropertiesFromMap(_, idName, _, true) => Some(idName)
              case SetNodePropertiesFromMap(_, idName, expr, _) if !expr.isInstanceOf[MapExpression] => Some(idName)
              case SetRelationshipPropertiesFromMap(_, idName, _, true) => Some(idName)
              case SetRelationshipPropertiesFromMap(_, idName, expr, _) if !expr.isInstanceOf[MapExpression] => Some(idName)
              case _ => None
            }

          val outgoingAvailableProperties = acc.availableProperties ++ newPropertyExpressions ++ propertiesFromPlan

          Acc(outgoingVariableOptima.toMap, outgoingReadOptima, outgoingAvailableProperties, acc.availableWholeEntities ++ maybeEntityFromPlan, outgoingCardinality)
      }
    }

    def foldTwoChildPlan(lhsAcc: Acc, rhsAcc: Acc, plan: LogicalPlan): Acc = {
      plan match {

          // Do _not_ pushdown from on top of these plans to the LHS or the RHS
        case _: Union |
             _: OrderedUnion |
          // TransactionForeach will clear caches, so it's useless to push down reads
             _: TransactionForeach =>
          val newVariables = plan.availableSymbols
          val outgoingCardinality = effectiveCardinalities(plan.id)
          val outgoingVariableOptima = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v))).toMap
          Acc(
            // Keep only optima of variables introduced in these plans
            outgoingVariableOptima,
            // Keep any pushdowns identified in either side
            lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima,
            // Keep no available properties/entities, which allows pushing down on top of these plans
            Set.empty,
            Set.empty,
            outgoingCardinality
          )

          // Do not pushdown from on top of these plans to the RHS, but allow pushing down to the LHS
        case _: AbstractSemiApply
           | _: AbstractLetSemiApply
           | _: AbstractSelectOrSemiApply
           | _: AbstractLetSelectOrSemiApply
           | _: ForeachApply
           | _: RollUpApply =>
          val mergedAcc = Acc(
            // Keep only optima for variables from the LHS
            lhsAcc.variableOptima,
            // Keep any pushdowns identified in either side
            lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima,
            // Keep only the available properties/entities from the LHS
            lhsAcc.availableProperties,
            lhsAcc.availableWholeEntities,
            effectiveCardinalities(plan.id)
          )
          foldSingleChildPlan(mergedAcc, plan)

        case _: ApplyPlan =>
          foldSingleChildPlan(rhsAcc, plan)

        case _ =>
          val mergedVariableOptima =
            lhsAcc.variableOptima ++ rhsAcc.variableOptima.map {
              case (v, rhsOptimum) =>
                lhsAcc.variableOptima.get(v) match {
                  case Some(lhsOptimum) =>
                    (v, Seq(lhsOptimum, rhsOptimum).minBy(_.cardinality))
                  case None =>
                    (v, rhsOptimum)
                }
            }

          Acc(mergedVariableOptima,
            lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima,
            lhsAcc.availableProperties ++ rhsAcc.availableProperties,
            lhsAcc.availableWholeEntities ++ rhsAcc.availableWholeEntities,
            effectiveCardinalities(plan.id))
      }
    }

    def mapArguments(argumentAcc: Acc, plan: LogicalPlan): Acc = {
      plan match {
        case _:TransactionForeach =>
          Acc(
            // Keep no optima of variables so that no property read can get pushed down from RHS to LHS
            Map.empty,
            // Keep any pushdowns identified so far
            argumentAcc.propertyReadOptima,
            // Keep no available properties/entities, which allows pushing down within the RHS
            Set.empty,
            Set.empty,
            argumentAcc.incomingCardinality
          )
        case _ => argumentAcc
      }
    }

    val Acc(_, propertyReadOptima, _, _, _) =
      LogicalPlans.foldPlan(Acc(Map.empty, Seq.empty, Set.empty, Set.empty, EffectiveCardinality(1)))(
        logicalPlan,
        foldSingleChildPlan,
        foldTwoChildPlan,
        mapArguments
      )

    val propertyMap = new mutable.HashMap[Id, Set[Property]].withDefaultValue(Set.empty)
    propertyReadOptima foreach {
      case (CardinalityOptimum(_, id, variableNameAtOptimum), property) =>
        propertyMap(id) += propertyWithName(variableNameAtOptimum, property)
    }

    val propertyReadInsertRewriter = bottomUp(Rewriter.lift {
      case lp: LogicalPlan if propertyMap.contains(lp.id) =>
        val copiedProperties = propertyMap(lp.id).map {
          p =>
            p.copy()(p.position): LogicalProperty
        }
        CacheProperties(lp, copiedProperties)(attributes.copy(lp.id))
    })

    propertyReadInsertRewriter(logicalPlan).asInstanceOf[LogicalPlan]
  }

  private def asProperty(idName: String)(indexedProperty: IndexedProperty): Property =
    Property(Variable(idName)(InputPosition.NONE), PropertyKeyName(indexedProperty.propertyKeyToken.name)(InputPosition.NONE))(InputPosition.NONE)

  private def propertyWithName(idName: String, p: Property): Property =
    p match {
      case Property(v: LogicalVariable, propertyKey) =>
        if (v.name == idName)
          p
        else
          Property(Variable(idName)(InputPosition.NONE), propertyKey)(InputPosition.NONE)
      case _ => throw new InternalException(s"Unexpected property read of non-variable `${p.map}`")
    }

  private def propertiesFromMap(idName: String, map: MapExpression): Seq[Property] = {
    map.items.map {
      case (prop, _) =>
        Property(Variable(idName)(InputPosition.NONE),prop)(InputPosition.NONE)
    }
  }
}

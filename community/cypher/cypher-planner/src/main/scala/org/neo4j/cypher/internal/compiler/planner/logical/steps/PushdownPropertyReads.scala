/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attributes, Id}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, InputPosition, Rewriter, bottomUp}
import org.neo4j.exceptions.InternalException

import scala.collection.mutable

case object PushdownPropertyReads {

  def pushdown(logicalPlan: LogicalPlan,
               cardinalities: Cardinalities,
               attributes: Attributes[LogicalPlan],
               semanticTable: SemanticTable): LogicalPlan = {

    def isNodeOrRel(variable: LogicalVariable): Boolean =
      semanticTable.types.get(variable)
        .exists(t => t.actual == CTNode.invariant || t.actual == CTRelationship.invariant)

    case class CardinalityOptimum(cardinality: Cardinality, logicalPlanId: Id, variableName: String)
    case class Acc(variableOptima: Map[String, CardinalityOptimum],
                   propertyReadOptima: Seq[(CardinalityOptimum, Property)],
                   availableProperties: Set[Property],
                   incomingCardinality: Cardinality)

    val Acc(_, propertyReadOptima, _, _) =
      LogicalPlans.foldPlan(Acc(Map.empty, Seq.empty, Set.empty, Cardinality.SINGLE))(
        logicalPlan,
        (acc, plan) => {
          val propertiesForPlan =
            plan.treeFold(List.empty[Property]) {
              case lp: LogicalPlan if lp.id != plan.id =>
                acc2 => (acc2, None) // do not traverse further
              case p @ Property(v: LogicalVariable, _) if isNodeOrRel(v) =>
                acc2 => (p :: acc2, Some(acc3 => acc3) )
            }

          val newPropertyReadOptima =
            propertiesForPlan.flatMap {
              case p @ Property(v: LogicalVariable, _) =>
                acc.variableOptima.get(v.name) match {
                  case Some(optimum: CardinalityOptimum) =>
                    if (optimum.cardinality < acc.incomingCardinality && !acc.availableProperties.contains(p))
                      Some((optimum, p))
                    else
                      None
                  // this happens for variables introduced in expressions, we ignore those for now
                  case None => None
                }
            }

          val outgoingCardinality = cardinalities(plan.id)
          val outgoingReadOptima = acc.propertyReadOptima ++ newPropertyReadOptima

          // TODO: handle aliasing in projections

          plan match {
            case _: Aggregation |
                 _: OrderedAggregation =>
              val newVariables = plan.availableSymbols
              val outgoingVariableOptima = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v))).toMap

              Acc(outgoingVariableOptima, outgoingReadOptima, Set.empty, outgoingCardinality)

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
                acc.availableProperties.map(
                  prop => {
                    val propVariable = prop.map.asInstanceOf[LogicalVariable].name
                    renamings.get(propVariable) match {
                      case Some(newName) => propertyWithName(newName, prop)
                      case None => prop
                    }
                  })

              Acc(renamedVariableOptima, outgoingReadOptima, renamedAvailableProperties, outgoingCardinality)

            case _ =>
              val newLowestCardinalities =
                acc.variableOptima.mapValues(optimum =>
                  if (outgoingCardinality < optimum.cardinality) {
                    CardinalityOptimum(outgoingCardinality, plan.id, optimum.variableName)
                  } else {
                    optimum
                  }
                )

              val currentVariables = plan.availableSymbols
              val newVariables = currentVariables -- acc.variableOptima.keySet
              val newVariableCardinalities = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v)))
              val outgoingVariableOptima = newLowestCardinalities ++ newVariableCardinalities

              val propertiesFromIndex: Seq[Property] =
                plan match {
                  case indexPlan: IndexLeafPlan =>
                    indexPlan.properties
                      .filter(_.getValueFromIndex == CanGetValue) // NOTE: as we pushdown before inserting cached properties
                                                                  //       the getValue behaviour will still be CanGetValue
                                                                  //       instead of GetValue
                      .map(asProperty(indexPlan.idName))
                  case _ => Seq.empty
                }
              val outgoingAvailableProperties = acc.availableProperties ++ propertiesForPlan ++ propertiesFromIndex

              Acc(outgoingVariableOptima, outgoingReadOptima, outgoingAvailableProperties, outgoingCardinality)
          }
        },
        (lhsAcc, rhsAcc, plan) => {

          plan match {
            case _: Union =>
              val newVariables = plan.availableSymbols
              val outgoingCardinality = cardinalities(plan.id)
              val outgoingVariableOptima = newVariables.map(v => (v, CardinalityOptimum(outgoingCardinality, plan.id, v))).toMap
              Acc(outgoingVariableOptima, lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima, Set.empty, outgoingCardinality)

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
                  cardinalities(plan.id))
          }
        }
      )

    val propertyMap = new mutable.HashMap[Id, Set[LogicalProperty]]
    propertyReadOptima foreach {
      case (CardinalityOptimum(_, id, variableNameAtOptimum), property) =>
        propertyMap(id) = propertyMap.getOrElse(id, Set.empty) + propertyWithName(variableNameAtOptimum, property)
    }

    val propertyReadInsertRewriter = bottomUp(Rewriter.lift {
      case lp: LogicalPlan if propertyMap.contains(lp.id) =>
        CacheProperties(lp, propertyMap(lp.id))(attributes.copy(lp.id))
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
}

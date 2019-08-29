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
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalProperty, LogicalVariable, Property, Variable}
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attributes, Id}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Rewriter, bottomUp}

import scala.collection.mutable

case object PushdownPropertyReads {

  def pushdown(logicalPlan: LogicalPlan,
               cardinalities: Cardinalities,
               attributes: Attributes[LogicalPlan],
               semanticTable: SemanticTable): LogicalPlan = {

    def isNodeOrRel(variable: LogicalVariable): Boolean =
      semanticTable.types.get(variable)
        .exists(t => t.actual == CTNode.invariant || t.actual == CTRelationship.invariant)

    case class VarLowestCardinality(lowestCardinality: Cardinality, logicalPlanId: Id)
    case class Acc(variableOptima: Map[String, VarLowestCardinality],
                   propertyReadOptima: Seq[(Id, Property)],
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
                  case Some(VarLowestCardinality(lowestCardinality, logicalPlanId)) =>
                    if (lowestCardinality < acc.incomingCardinality && !acc.availableProperties.contains(p))
                      Some((logicalPlanId, p))
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
              val outgoingVariableOptima = newVariables.map(v => (v, VarLowestCardinality(outgoingCardinality, plan.id))).toMap

              Acc(outgoingVariableOptima, outgoingReadOptima, Set.empty, outgoingCardinality)
            case _ =>
              val newLowestCardinalities =
                acc.variableOptima.mapValues(x =>
                  if (outgoingCardinality < x.lowestCardinality) {
                    VarLowestCardinality(outgoingCardinality, plan.id)
                  } else {
                    x
                  }
                )

              val currentVariables = plan.availableSymbols
              val newVariables = currentVariables -- acc.variableOptima.keySet
              val newVariableCardinalities = newVariables.map(v => (v, VarLowestCardinality(outgoingCardinality, plan.id)))
              val outgoingVariableOptima = newLowestCardinalities ++ newVariableCardinalities

              Acc(outgoingVariableOptima, outgoingReadOptima, acc.availableProperties ++ propertiesForPlan, outgoingCardinality)
          }
        },
        (lhsAcc, rhsAcc, plan) => {

          plan match {
            case _: Union =>
              val newVariables = plan.availableSymbols
              val outgoingCardinality = cardinalities(plan.id)
              val outgoingVariableOptima = newVariables.map(v => (v, VarLowestCardinality(outgoingCardinality, plan.id))).toMap
              Acc(outgoingVariableOptima, lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima, Set.empty, outgoingCardinality)

            case _ =>
              val mergedVariableOptima =
                lhsAcc.variableOptima ++ rhsAcc.variableOptima.map {
                  case (v, rhsOptimum) =>
                    lhsAcc.variableOptima.get(v) match {
                  case Some(lhsOptimum) =>
                    (v, Seq(lhsOptimum, rhsOptimum).minBy(_.lowestCardinality))
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
      case (id, property) =>
        propertyMap(id) = propertyMap.getOrElse(id, Set.empty) + property
    }

    val propertyReadInsertRewriter = bottomUp(Rewriter.lift {
      case lp: LogicalPlan if propertyMap.contains(lp.id) =>
        CacheProperties(lp, propertyMap(lp.id))(attributes.copy(lp.id))
    })

    propertyReadInsertRewriter(logicalPlan).asInstanceOf[LogicalPlan]
  }
}

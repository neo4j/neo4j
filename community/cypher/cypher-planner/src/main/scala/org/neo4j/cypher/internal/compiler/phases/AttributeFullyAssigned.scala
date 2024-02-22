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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.attribution.Attribute

import scala.reflect.ClassTag

case class AttributeFullyAssigned[T <: Attribute[LogicalPlan, _]]()(implicit val tag: ClassTag[T])
    extends ValidatingCondition {

  override def apply(in: Any)(cancellationChecker: CancellationChecker): Seq[String] = in match {
    case state: LogicalPlanState =>
      val plan = state.logicalPlan
      val attribute = tag.runtimeClass match {
        case x if classOf[Solveds] == x                => state.planningAttributes.solveds
        case x if classOf[Cardinalities] == x          => state.planningAttributes.cardinalities
        case x if classOf[EffectiveCardinalities] == x => state.planningAttributes.effectiveCardinalities
        case x if classOf[ProvidedOrders] == x         => state.planningAttributes.providedOrders
        case x if classOf[LeveragedOrders] == x        => state.planningAttributes.leveragedOrders
        case x                                         => throw new IllegalArgumentException(s"Unknown attribute: $x")
      }

      plan.folder(cancellationChecker).treeFold(Seq.empty[String]) {
        case plan: LogicalPlan => acc =>
            if (!attribute.isDefinedAt(plan.id)) {
              val error =
                s"Attribute ${tag.runtimeClass.getSimpleName} not set for \n${LogicalPlanToPlanBuilderString(plan)}"
              TraverseChildren(acc :+ error)
            } else {
              TraverseChildren(acc)
            }
      }

    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }

  override def name: String = s"$productPrefix[${tag.runtimeClass.getSimpleName}]"

  override def hashCode(): Int = tag.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case cc: AttributeFullyAssigned[_] => tag.equals(cc.tag)
    case _                             => false
  }
}

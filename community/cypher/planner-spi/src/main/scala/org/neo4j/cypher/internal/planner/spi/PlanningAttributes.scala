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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.PartialAttribute

object PlanningAttributes {
  class Solveds extends Attribute[LogicalPlan, PlannerQuery]
  class Cardinalities extends Attribute[LogicalPlan, Cardinality]
  class EffectiveCardinalities extends Attribute[LogicalPlan, EffectiveCardinality]
  class ProvidedOrders extends Attribute[LogicalPlan, ProvidedOrder]
  class LeveragedOrders extends PartialAttribute[LogicalPlan, Boolean](false)
  class LabelAndRelTypeInfos extends PartialAttribute[LogicalPlan, Option[LabelAndRelTypeInfo]](None)

  def newAttributes: PlanningAttributes = PlanningAttributes(
    new Solveds,
    new Cardinalities,
    new EffectiveCardinalities,
    new ProvidedOrders,
    new LeveragedOrders,
    new LabelAndRelTypeInfos
  )
}

/**
 * 
 * @param solveds the planner query that each plan solves.
 * @param cardinalities cardinality estimation for each plan.
 * @param effectiveCardinalities effective cardinality estimation (taking LIMIT into account) for each plan.
 * @param providedOrders provided order for each plan
 * @param leveragedOrders a boolean flag if the plan leverages order of rows.
 * @param labelAndRelTypeInfos label and reltype info that is valid at the location of the plan.
 *                             Currently this is only set for Selection plans.
 */
case class PlanningAttributes(
  solveds: Solveds,
  cardinalities: Cardinalities,
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders,
  labelAndRelTypeInfos: LabelAndRelTypeInfos
) {
  private val attributes = productIterator.asInstanceOf[Iterator[Attribute[LogicalPlan, _]]].toSeq

  def asAttributes(idGen: IdGen): Attributes[LogicalPlan] = Attributes[LogicalPlan](idGen, attributes: _*)

  // Let's not override the copy method of case classes
  def createCopy(): PlanningAttributes =
    PlanningAttributes(
      solveds.clone[Solveds],
      cardinalities.clone[Cardinalities],
      effectiveCardinalities.clone[EffectiveCardinalities],
      providedOrders.clone[ProvidedOrders],
      leveragedOrders.clone[LeveragedOrders],
      labelAndRelTypeInfos.clone[LabelAndRelTypeInfos]
    )

  def hasEqualSizeAttributes: Boolean = {
    val fullAttributes = attributes.filter(!_.isInstanceOf[PartialAttribute[_, _]])
    fullAttributes.tail.forall(_.size == fullAttributes.head.size)
  }

  def cacheKey: PlanningAttributesCacheKey = {
    PlanningAttributesCacheKey(cardinalities, effectiveCardinalities, providedOrders, leveragedOrders)
  }
}

/**
 * This case class captures all PlanningAttributes needed for computing an ExecutionPlan.
 * This class is also used in place of the original PlanningAttributes in `computeExecutionPlan` so that we by type
 * checking know that the key includes all PlanningAttributes that are needed for computing an execution plan.
 */
case class PlanningAttributesCacheKey(
  cardinalities: Cardinalities,
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders
)

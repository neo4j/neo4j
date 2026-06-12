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
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes.EffectiveCardinalities.MISSING
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.CachedPropertiesPerPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.PartialAttribute

import scala.collection.immutable.ArraySeq
import scala.collection.immutable.BitSet

object PlanningAttributes {
  class Solveds extends Attribute[LogicalPlan, PlannerQuery]
  class Cardinalities extends Attribute[LogicalPlan, Cardinality]
  class EffectiveCardinalities extends Attribute[LogicalPlan, EffectiveCardinality]
  class ProvidedOrders extends Attribute[LogicalPlan, ProvidedOrder]
  class LeveragedOrders extends PartialAttribute[LogicalPlan, Boolean](false)
  class StableLeafPlans extends PartialAttribute[LogicalPlan, LeafStability](LeafStability.NonMvcc)
  class LabelAndRelTypeInfos extends PartialAttribute[LogicalPlan, Option[LabelAndRelTypeInfo]](None)
  class CachedPropertiesPerPlan extends PartialAttribute[LogicalPlan, CachedProperties](CachedProperties.empty)

  def newAttributes: PlanningAttributes = PlanningAttributes(
    new Solveds,
    new Cardinalities,
    new EffectiveCardinalities,
    new ProvidedOrders,
    new LeveragedOrders,
    new StableLeafPlans,
    new LabelAndRelTypeInfos,
    new CachedPropertiesPerPlan
  )
}

/**
 *
 * @param solveds the planner query that each plan solves.
 * @param cardinalities cardinality estimation for each plan.
 * @param effectiveCardinalities effective cardinality estimation (taking LIMIT into account) for each plan.
 * @param providedOrders provided order for each plan
 * @param leveragedOrders a boolean flag if the plan leverages order of rows.
 * @param stableLeafPlans the stable-iterator classification of a leaf plan (defaults to NonMvcc).
 * @param labelAndRelTypeInfos label and reltype info that is valid at the location of the plan.
 *                             Currently, this is only set for Selection plans.
 */
case class PlanningAttributes(
  solveds: Solveds,
  cardinalities: Cardinalities,
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders,
  stableLeafPlans: StableLeafPlans,
  labelAndRelTypeInfos: LabelAndRelTypeInfos,
  cachedPropertiesPerPlan: CachedPropertiesPerPlan
) {
  private val attributes = productIterator.asInstanceOf[Iterator[Attribute[LogicalPlan, ?]]].toSeq

  def asAttributes(idGen: IdGen): Attributes[LogicalPlan] = Attributes[LogicalPlan](idGen, attributes)
}

/**
 * This case class captures all PlanningAttributes needed for computing an ExecutionPlan, optimised for space.
 * This class is also used in place of the original PlanningAttributes in `computeExecutionPlan` so that we by type
 * checking know that the key includes all PlanningAttributes that are needed for computing an execution plan.
 */
case class PlanningAttributesCacheKey(
  effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
  providedOrders: ImmutablePlanningAttributes.ProvidedOrders,
  leveragedOrders: ImmutablePlanningAttributes.LeveragedOrders,
  stableLeafPlans: ImmutablePlanningAttributes.StableLeafPlans
)

trait ImmutablePlanningAttribute[T] {
  def isDefinedAt(id: Id): Boolean
  def get(id: Id): T
  def size: Int
}

object ImmutablePlanningAttributes {

  /**
   * Space optimized version of PlanningAttributes.EffectiveCardinalities,
   * intended to decrease heap usage of query cache.
   */
  final case class EffectiveCardinalities(
    private val amounts: ArraySeq[Double],
    private val originalAmounts: ArraySeq[Double]
  ) extends ImmutablePlanningAttribute[EffectiveCardinality] {
    override def isDefinedAt(id: Id): Boolean = id.x < amounts.size && amounts(id.x) != MISSING

    override def get(id: Id): EffectiveCardinality = {
      if (!isDefinedAt(id)) {
        throw new NoSuchElementException(s"Id $id")
      } else {
        EffectiveCardinality(
          amounts(id.x),
          Option.when(id.x < originalAmounts.size && originalAmounts(id.x) != MISSING) {
            Cardinality(originalAmounts(id.x))
          }
        )
      }
    }

    override def size: Int = amounts.count(_ != MISSING)

    def toMutable: PlanningAttributes.EffectiveCardinalities = {
      val mutable = new PlanningAttributes.EffectiveCardinalities
      mutable.sizeHint(amounts.size)
      var i = 0
      while (i < amounts.size) {
        val id = Id(i)
        if (isDefinedAt(id)) mutable.set(id, get(id))
        i += 1
      }
      mutable
    }
  }

  object EffectiveCardinalities {
    private val MISSING = Double.MinValue

    def apply(cs: PlanningAttributes.EffectiveCardinalities): ImmutablePlanningAttributes.EffectiveCardinalities = {
      val maxId = cs.iterator
        .map { case (id, _) => id.x + 1 }
        .maxOption
        .getOrElse(0)
      val amounts = Array.fill[Double](maxId)(MISSING)
      val originals = Array.fill[Double](maxId)(MISSING)
      cs.iterator.foreach { case (Id(id), c) =>
        amounts(id) = flatten(c.amount)
        originals(id) = c.originalCardinality match {
          case Some(Cardinality(original)) => flatten(original)
          case None                        => MISSING
        }
      }
      EffectiveCardinalities(ArraySeq.unsafeWrapArray(amounts), ArraySeq.unsafeWrapArray(originals))
    }

    // We don't expect negative cardinalities,
    // but if we do encounter a cardinality of Double.MinValue we remap it to math.nextUp(MISSING).
    // This is done to not hide unexpected cardinalities, caused by bugs, in plan descriptions.
    private def flatten(value: Double): Double = if (value == MISSING) math.nextUp(MISSING) else value
  }

  final case class ProvidedOrders private (
    private val orders: Map[Int, ProvidedOrder],
    override val size: Int
  ) extends ImmutablePlanningAttribute[ProvidedOrder] {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(size > orders.keySet.maxOption.getOrElse(-1))
    override def isDefinedAt(id: Id): Boolean = id.x < size && !orders.get(id.x).contains(null)
    override def get(id: Id): ProvidedOrder = orders.getOrElse(id.x, ProvidedOrder.empty)

    def toMutable: PlanningAttributes.ProvidedOrders = {
      val mutable = new PlanningAttributes.ProvidedOrders()
      mutable.sizeHint(size)
      Range(0, size)
        .filter(id => isDefinedAt(Id(id)))
        .foreach(id => mutable.set(Id(id), get(Id(id))))
      mutable
    }
  }

  object ProvidedOrders {

    def apply(mutable: PlanningAttributes.ProvidedOrders): ProvidedOrders = {
      val size = mutable.iterator
        .map { case (Id(id), _) => id + 1 }
        .maxOption
        .getOrElse(0)
      val map = Range(0, size)
        .map(Id.apply)
        .collect {
          case id if mutable.isDefinedAt(id) && mutable.get(id) != ProvidedOrder.empty => id.x -> mutable.get(id)
          case id if !mutable.isDefinedAt(id)                                          => id.x -> null
        }
        .toMap
      new ProvidedOrders(map, size)
    }
  }

  final case class LeveragedOrders private (private val leveraged: BitSet) {

    def toMutable: PlanningAttributes.LeveragedOrders = {
      val mutable = new PlanningAttributes.LeveragedOrders
      leveraged.maxOption.foreach(max => mutable.sizeHint(max))
      leveraged.foreach(id => mutable.set(Id(id), true))
      mutable
    }
  }

  object LeveragedOrders {

    def apply(mutable: PlanningAttributes.LeveragedOrders): LeveragedOrders = {
      val builder = BitSet.newBuilder
      mutable.iterator.foreach { case (Id(id), l) => if (l) builder.addOne(id) }
      new LeveragedOrders(builder.result())
    }
  }

  final case class StableLeafPlans private (private val stability: Map[Int, LeafStability]) {

    def toMutable: PlanningAttributes.StableLeafPlans = {
      val mutable = new PlanningAttributes.StableLeafPlans
      stability.keysIterator.maxOption.foreach(max => mutable.sizeHint(max))
      stability.foreach { case (id, value) => mutable.set(Id(id), value) }
      mutable
    }
  }

  object StableLeafPlans {

    def apply(mutable: PlanningAttributes.StableLeafPlans): StableLeafPlans = {
      val builder = Map.newBuilder[Int, LeafStability]
      mutable.iterator.foreach {
        case (Id(id), stability) if stability != LeafStability.NonMvcc => builder.addOne(id -> stability)
        case _                                                         =>
      }
      new StableLeafPlans(builder.result())
    }
  }
}

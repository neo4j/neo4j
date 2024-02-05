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
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.ExecutionModel.SelectedBatchSize
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.BatchedCartesianOrdering
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.CartesianOrdering
import org.neo4j.cypher.internal.util.VolcanoCartesianOrdering

/**
 * The execution model of how a runtime executes a query.
 * This information can be used in planning.
 */
sealed trait ExecutionModel {

  /**
   * @param maxCardinality the maximum cardinality when combining plans to a Cartesian Product with the returned ordering.
   * @return an Ordering for plans to combine them with Cartesian Products.
   */
  def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering

  def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: Cardinalities): SelectedBatchSize

  /**
   * In single-threaded runtimes with a deterministic execution order, the order of rows will be preserved, whereas
   * in parallel runtime, batches of rows may arrive to downstream operators out-of-order.
   *
   * @return true if the execution model supports maintaining a provided order through downstream operators
   */
  def providedOrderPreserving: Boolean

  /**
   * @return true if the execution model does not maintain provided order for a specific logical plan (not recursive)
   *
   * This mainly targets plans that in a particular execution model invalidates the order of arguments rows on
   * the right-hand side of an Apply. In addition to this there are also other more general rules for how plans
   * affects provided order.
   *
   * The check is invoked on each plan under an Apply and the implementation is not expected to recurse into it children.
   */
  def invalidatesProvidedOrder(plan: LogicalPlan): Boolean

  /**
   * @return any fields that are relevant for caching
   */
  def cacheKey(): Seq[Any]
}

object ExecutionModel {
  val default: ExecutionModel = Batched.default

  case object Volcano extends ExecutionModel {
    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering = VolcanoCartesianOrdering

    override def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: Cardinalities): SelectedBatchSize =
      VolcanoBatchSize
    override def providedOrderPreserving: Boolean = true
    override def invalidatesProvidedOrder(plan: LogicalPlan): Boolean = false

    /**
     * We do not include the ExecutionModel itself, since we also have a compiler for each runtime (see CompilerLibrary).
     */
    override def cacheKey(): Seq[Any] = Seq.empty
  }

  case class BatchedSingleThreaded(smallBatchSize: Int, bigBatchSize: Int) extends Batched {
    override def providedOrderPreserving: Boolean = true

    /**
     * We do not include the ExecutionModel itself, since we also have a compiler for each runtime (see CompilerLibrary).
     */
    override def cacheKey(): Seq[Any] = this match {
      // Note: This extra match is here to trigger a compilation error whenever the Signature of Settings is changed,
      // to make the author aware and make them think about whether they want to include a new field in the cache key.
      case BatchedSingleThreaded(
          smallBatchSize: Int,
          bigBatchSize: Int
        ) =>
        val builder = Seq.newBuilder[Any]

        if (GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.dynamic())
          builder.addOne(smallBatchSize)

        if (GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.dynamic())
          builder.addOne(bigBatchSize)

        builder.result()
    }
  }

  case class BatchedParallel(smallBatchSize: Int, bigBatchSize: Int) extends Batched {
    override def providedOrderPreserving: Boolean = false

    /**
     * We do not include the ExecutionModel itself, since we also have a compiler for each runtime (see CompilerLibrary).
     */
    override def cacheKey(): Seq[Any] = this match {
      // Note: This extra match is here to trigger a compilation error whenever the Signature of Settings is changed,
      // to make the author aware and make them think about whether they want to include a new field in the cache key.
      case BatchedParallel(
          smallBatchSize: Int,
          bigBatchSize: Int
        ) =>
        val builder = Seq.newBuilder[Any]

        if (GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.dynamic())
          builder.addOne(smallBatchSize)

        if (GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.dynamic())
          builder.addOne(bigBatchSize)

        builder.result()
    }
  }

  abstract class Batched extends ExecutionModel {
    def smallBatchSize: Int
    def bigBatchSize: Int

    /**
     * Select the batch size for executing a logical plan.
     */
    def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: Cardinalities): SelectedBatchSize = {
      val maxCardinality = logicalPlan.flatten.map(plan => cardinalities.get(plan.id)).max
      BatchedBatchSize(selectBatchSize(maxCardinality.amount))
    }

    /**
     * Select the batch size for executing a logical plan.
     */
    def selectBatchSize(
      logicalPlan: LogicalPlan,
      cardinalities: EffectiveCardinalities
    ): Int = {
      val maxCardinality = logicalPlan.flatten.map(plan => cardinalities.get(plan.id)).max
      val selectedBatchSize = selectBatchSize(maxCardinality.amount)
      selectedBatchSize
    }

    private def selectBatchSize(maxCardinality: Double): Int = {
      if (maxCardinality.toLong > bigBatchSize) bigBatchSize else smallBatchSize
    }

    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering =
      new BatchedCartesianOrdering(selectBatchSize(maxCardinality.amount))

    override def invalidatesProvidedOrder(plan: LogicalPlan): Boolean = plan match {
      case _: Union =>
        true
      case _ =>
        false
    }
  }

  object Batched {

    val default: Batched = BatchedSingleThreaded(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.defaultValue(),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.defaultValue()
    )
  }

  trait SelectedBatchSize {
    def size: Cardinality
    def numBatchesFor(cardinality: Cardinality): Cardinality
  }

  case object VolcanoBatchSize extends SelectedBatchSize {
    val size: Cardinality = 1
    def numBatchesFor(cardinality: Cardinality): Cardinality = cardinality
  }

  private case class BatchedBatchSize(size: Cardinality) extends SelectedBatchSize {
    def numBatchesFor(cardinality: Cardinality): Cardinality = (cardinality * size.inverse).ceil
  }

}

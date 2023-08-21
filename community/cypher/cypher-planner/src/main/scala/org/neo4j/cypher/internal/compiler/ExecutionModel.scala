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
import org.neo4j.exceptions.CantCompileQueryException

import scala.annotation.tailrec

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
      cardinalities: EffectiveCardinalities,
      explicitBatchSize: Option[Long]
    ): Int = {
      explicitBatchSize match {
        case Some(explicitSize) =>
          val fittedBatchSize = fitBatchSize(explicitSize)
          fittedBatchSize.getOrElse(
            throw new CantCompileQueryException(s"The periodic commit batch size $explicitSize is not supported")
          )

        case None =>
          val maxCardinality = logicalPlan.flatten.map(plan => cardinalities.get(plan.id)).max
          val selectedBatchSize = selectBatchSize(maxCardinality.amount)
          selectedBatchSize
      }
    }

    private def fitBatchSize(explicitSize: Long): Option[Int] = {
      // We use bigBatchSize*2 as the upper limit of explicit batch sizes that we honor exactly so that we do not end up with unreasonably big morsels
      // (Also set a minimum of 4 for very small values of bigBatchSize, since otherwise fitting will not work)
      val batchSizeUpperLimit = Math.max(bigBatchSize * 2, 4)
      val batchSizeLowerLimit = smallBatchSize

      if (explicitSize > 0 && explicitSize <= batchSizeUpperLimit) {
        // Use the exact value
        Some(explicitSize.toInt)
      } else if ((explicitSize / batchSizeUpperLimit) > 10000L) {
        // Do not even try to fit the explicit size if it is big enough
        // In this case the difference of one batch is unlikely to matter, and the fitting algorithm may not complete in a timely manner
        Some(bigBatchSize)
      } else {
        // Try to find a suitable batch size that is a factor of the explicit batch size or a nearby number
        def constraint1(n: Long): Boolean = {
          n <= batchSizeUpperLimit && n >= batchSizeLowerLimit
        }
        def constraint2(n: Long): Boolean = {
          n <= batchSizeUpperLimit
        }
        val constraints = Array[Long => Boolean](constraint1, constraint2)

        var attempt = 0
        while (attempt < constraints.length) {
          var n = explicitSize
          val lowerLimit = Math.max(1, explicitSize - 8)
          while (n > lowerLimit) {
            val fittedBatchSize = tryFitBachSize(constraints(attempt))(n)
            if (fittedBatchSize.isDefined) {
              return fittedBatchSize
            }
            n -= 1
          }
          attempt += 1
        }
        None
      }
    }

    private def tryFitBachSize(p: Long => Boolean)(n: Long): Option[Int] = {
      @tailrec
      def fit(n: Long, fac: Long = 2): Option[Int] = {
        fac + fac > n match {
          case false if n % fac == 0 && p(n / fac) => Some((n / fac).toInt)
          case false                               => fit(n, fac + 1)
          case true                                => None
        }
      }
      fit(n)
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

  case class BatchedBatchSize(size: Cardinality) extends SelectedBatchSize {
    def numBatchesFor(cardinality: Cardinality): Cardinality = (cardinality * size.inverse).ceil
  }

}

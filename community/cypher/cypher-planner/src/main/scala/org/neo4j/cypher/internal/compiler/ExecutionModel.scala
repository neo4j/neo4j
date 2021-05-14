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
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.ExecutionModel.SelectedBatchSize
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
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
}

object ExecutionModel {
  val default: ExecutionModel = Batched.default

  case object Volcano extends ExecutionModel {
    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering = VolcanoCartesianOrdering
    override def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: Cardinalities): SelectedBatchSize = VolcanoBatchSize
  }

  case class Batched(smallBatchSize: Int, bigBatchSize: Int) extends ExecutionModel {

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
    def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: EffectiveCardinalities, explicitBatchSize: Option[Long]): Int = {
      explicitBatchSize match {
        case Some(explicitSize) =>
          val fittedBatchSize = fitBatchSize(explicitSize)
          fittedBatchSize.getOrElse(throw new CantCompileQueryException(s"The periodic commit batch size $explicitSize is not supported"))

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
          case false                 => fit(n, fac + 1)
          case true                  => None
        }
      }
      fit(n)
    }

    private def selectBatchSize(maxCardinality: Double): Int = {
      if (maxCardinality.toLong > bigBatchSize) bigBatchSize else smallBatchSize
    }

    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering = new BatchedCartesianOrdering(selectBatchSize(maxCardinality.amount))
  }

  object Batched {
    val default: Batched = Batched(
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

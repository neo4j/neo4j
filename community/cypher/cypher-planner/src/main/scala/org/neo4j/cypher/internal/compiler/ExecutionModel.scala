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
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
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
}

object ExecutionModel {
  val default: ExecutionModel = Batched.default

  case object Volcano extends ExecutionModel {
    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering = VolcanoCartesianOrdering
  }

  case class Batched(smallBatchSize: Int, bigBatchSize: Int) extends ExecutionModel {

    /**
     * Select the batch size for executing a logical plan.
     */
    def selectBatchSize(logicalPlan: LogicalPlan, cardinalities: Cardinalities): Int = {
      val maxCardinality = logicalPlan.flatten.map(plan => cardinalities.get(plan.id)).max
      selectBatchSize(maxCardinality)
    }

    private def selectBatchSize(maxCardinality: Cardinality): Int = {
      if (maxCardinality.amount.toLong > bigBatchSize) bigBatchSize else smallBatchSize
    }

    override def cartesianOrdering(maxCardinality: Cardinality): CartesianOrdering = new BatchedCartesianOrdering(selectBatchSize(maxCardinality))
  }

  object Batched {
    val default: Batched = Batched(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.defaultValue(),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.defaultValue()
    )
  }
}

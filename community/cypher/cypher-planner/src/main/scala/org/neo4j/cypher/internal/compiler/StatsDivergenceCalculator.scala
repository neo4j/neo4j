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

import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherReplanAlgorithm
import org.neo4j.cypher.internal.config.StatsDivergenceCalculatorConfig

object StatsDivergenceCalculator {
  val similarityTolerance = 0.0001

  def divergenceCalculatorFor(config: StatsDivergenceCalculatorConfig): StatsDivergenceCalculator = {
    val StatsDivergenceCalculatorConfig(
      algorithm,
      initialThreshold,
      targetThreshold,
      minReplanInterval,
      targetReplanInterval
    ) = config
    divergenceCalculatorFor(algorithm, initialThreshold, targetThreshold, minReplanInterval, targetReplanInterval)
  }

  def divergenceCalculatorFor(
    algorithm: CypherReplanAlgorithm,
    initialThreshold: Double,
    targetThreshold: Double,
    minReplanInterval: Long,
    targetReplanInterval: Long
  ): StatsDivergenceCalculator = {
    if (
      targetThreshold <= similarityTolerance || initialThreshold - targetThreshold <= similarityTolerance || targetReplanInterval <= minReplanInterval
    ) {
      // Input values that disable the threshold decay algorithm
      StatsDivergenceNoDecayCalculator(initialThreshold, minReplanInterval)
    } else {
      // Input is valid, select decay algorithm, the GraphDatabaseSettings will limit the possible values
      algorithm match {
        case CypherReplanAlgorithm.NONE => StatsDivergenceNoDecayCalculator(initialThreshold, minReplanInterval)
        case CypherReplanAlgorithm.EXPONENTIAL => StatsDivergenceExponentialDecayCalculator(
            initialThreshold,
            targetThreshold,
            minReplanInterval,
            targetReplanInterval
          )
        case CypherReplanAlgorithm.INVERSE |
          CypherReplanAlgorithm.DEFAULT => StatsDivergenceInverseDecayCalculator(
            initialThreshold,
            targetThreshold,
            minReplanInterval,
            targetReplanInterval
          )
      }
    }
  }

  def divergenceNoDecayCalculator(threshold: Double, ttl: Long): StatsDivergenceNoDecayCalculator =
    StatsDivergenceNoDecayCalculator(threshold, ttl)
}

sealed trait StatsDivergenceCalculator {
  def initialThreshold: Double
  def minReplanInterval: Long

  def shouldCheck(currentTimeMillis: Long, lastCheckTimeMillis: Long): Boolean =
    currentTimeMillis - lastCheckTimeMillis >= minReplanInterval

  def decay(millisSincePreviousReplan: Long): Double
}

case class StatsDivergenceInverseDecayCalculator(
  initialThreshold: Double,
  targetThreshold: Double,
  minReplanInterval: Long,
  targetReplanInterval: Long
) extends StatsDivergenceCalculator {
  val decayFactor: Double = (initialThreshold / targetThreshold - 1.0) / (targetReplanInterval - minReplanInterval)

  def decay(millisSincePreviousReplan: Long): Double = {
    // Note that this equation has a possible singularity for very steep decays, when millisSincePreviousReplan < minReplanInterval
    // However, that will never happen because of the 'shouldCheck' test above
    initialThreshold / (1.0 + decayFactor * (millisSincePreviousReplan - minReplanInterval))
  }
}

case class StatsDivergenceExponentialDecayCalculator(
  initialThreshold: Double,
  targetThreshold: Double,
  minReplanInterval: Long,
  targetReplanInterval: Long
) extends StatsDivergenceCalculator {

  val decayFactor: Double =
    (Math.log(initialThreshold) - Math.log(targetThreshold)) / (targetReplanInterval - minReplanInterval)

  def decay(millisSincePreviousReplan: Long): Double = {
    val exponent = -1.0 * decayFactor * (millisSincePreviousReplan - minReplanInterval)
    initialThreshold * Math.exp(exponent)
  }
}

case class StatsDivergenceNoDecayCalculator(initialThreshold: Double, minReplanInterval: Long)
    extends StatsDivergenceCalculator {

  def decay(millisSinceThreshold: Long): Double = {
    initialThreshold
  }
}

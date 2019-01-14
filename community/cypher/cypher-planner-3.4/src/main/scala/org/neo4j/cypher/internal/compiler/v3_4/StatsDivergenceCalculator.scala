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
package org.neo4j.cypher.internal.compiler.v3_4

sealed trait CacheCheckResult

case class NeedsReplan(secondsSinceReplan: Int) extends CacheCheckResult
case object FineToReuse extends CacheCheckResult

sealed trait StatsDivergenceCalculator {
  val initialThreshold: Double
  val initialMillis: Long

  def shouldCheck(currentTimeMillis: Long, lastCheckTimeMillis: Long): Boolean = currentTimeMillis - initialMillis >= lastCheckTimeMillis

  def decay(millisSincePreviousReplan: Long): Double
}

case class StatsDivergenceInverseDecayCalculator(initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long) extends StatsDivergenceCalculator {
  val decayFactor: Double = (initialThreshold / targetThreshold - 1.0) / (targetMillis - initialMillis)

  def decay(millisSincePreviousReplan: Long): Double = {
    // Note that this equation has a possible singularity for very steep decays, when millisSincePreviousReplan < initialMillis
    // However, that will never happen because of the 'tooSoon' test above
    initialThreshold / (1.0 + decayFactor * (millisSincePreviousReplan - initialMillis))
  }
}

case class StatsDivergenceExponentialDecayCalculator(initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long) extends StatsDivergenceCalculator {
  val decayFactor: Double = (Math.log(initialThreshold) - Math.log(targetThreshold)) / (targetMillis - initialMillis)

  def decay(millisSincePreviousReplan: Long): Double = {
    val exponent = -1.0 * decayFactor * (millisSincePreviousReplan - initialMillis)
    initialThreshold * Math.exp(exponent)
  }
}

case class StatsDivergenceNoDecayCalculator(initialThreshold: Double, initialMillis: Long) extends StatsDivergenceCalculator {
  def decay(millisSinceThreshold: Long): Double = {
    initialThreshold
  }
}

object StatsDivergenceCalculator {
  val inverse = "inverse"
  val exponential = "exponential"
  val none = "none"
  val similarityTolerance = 0.0001

  def divergenceCalculatorFor(name: String, initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long): StatsDivergenceCalculator = {
    if (targetThreshold <= similarityTolerance || initialThreshold - targetThreshold <= similarityTolerance || targetMillis <= initialMillis) {
      // Input values that disable the threshold decay algorithm
      StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
    } else {
      // Input is valid, select decay algorithm, the GraphDatabaseSettings will limit the possible values
      name.toLowerCase match {
        case "none" => StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
        case "exponential" => StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
        case _ => StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
      }
    }
  }

  def divergenceNoDecayCalculator(threshold: Double, ttl: Long) =
    StatsDivergenceNoDecayCalculator(threshold, ttl)
}

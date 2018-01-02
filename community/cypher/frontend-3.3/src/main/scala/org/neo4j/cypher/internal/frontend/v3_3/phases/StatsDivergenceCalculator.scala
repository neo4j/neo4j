/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.phases

case class CacheCheckResult(isStale: Boolean, secondsSinceReplan: Int)

object CacheCheckResult {
  val empty = CacheCheckResult(isStale = false,0)
}

trait StatsDivergenceCalculator {
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

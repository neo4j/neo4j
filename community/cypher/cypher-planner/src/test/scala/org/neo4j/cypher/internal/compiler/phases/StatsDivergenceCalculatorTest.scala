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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherReplanAlgorithm
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatsDivergenceCalculatorTest extends CypherFunSuite {

  private val defaultInitialThreshold =
    GraphDatabaseSettings.query_statistics_divergence_threshold.defaultValue.toDouble

  private val defaultTargetThreshold =
    GraphDatabaseInternalSettings.query_statistics_divergence_target.defaultValue.toDouble
  private val defaultInitialInterval = GraphDatabaseSettings.cypher_min_replan_interval.defaultValue.toMillis
  private val defaultTargetInterval = GraphDatabaseInternalSettings.cypher_replan_interval_target.defaultValue.toMillis
  private val marginOfError = 0.0001

  test("Disabling decay should show no decay") {
    val divergence =
      StatsDivergenceCalculator.divergenceNoDecayCalculator(defaultInitialThreshold, defaultInitialInterval)
    assertNoDecay(CypherReplanAlgorithm.NONE, divergence, defaultInitialThreshold, defaultInitialInterval)
  }

  test("Using algorithm 'none' decay should show no decay") {
    assertNoDecay(
      CypherReplanAlgorithm.NONE,
      defaultInitialThreshold,
      defaultTargetThreshold,
      defaultInitialInterval,
      defaultTargetInterval
    )
  }

  test("Default values should make sense") {
    assertDecaysMakeSense(
      CypherReplanAlgorithm.DEFAULT,
      defaultInitialThreshold,
      defaultTargetThreshold,
      defaultInitialInterval,
      defaultTargetInterval
    )
  }

  test("Default values should make sense with inverse decay") {
    assertDecaysMakeSense(
      CypherReplanAlgorithm.INVERSE,
      defaultInitialThreshold,
      defaultTargetThreshold,
      defaultInitialInterval,
      defaultTargetInterval
    )
  }

  test("Default values should make sense with exponential decay") {
    assertDecaysMakeSense(
      CypherReplanAlgorithm.EXPONENTIAL,
      defaultInitialThreshold,
      defaultTargetThreshold,
      defaultInitialInterval,
      defaultTargetInterval
    )
  }

  test("Equal threshold should disable decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, 0.5, 0.5, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Increasing threshold should disable decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, 0.5, 0.75, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Target threshold of zero should disable decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, 0.5, 0.0, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Small target threshold should work") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertDecaysMakeSense(name, 0.5, 0.001, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Not changing time should disable decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 1000)
    }
  }

  test("Going back in time should disable decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 500)
    }
  }

  test("Small time interval should work") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertDecaysMakeSense(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 1002)
    }
  }

  test("A small threshold should not increase on decay") {
    Seq(CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL).foreach { name =>
      assertNoDecay(name, 0, defaultTargetThreshold, defaultInitialInterval, defaultTargetInterval)
    }
  }

  private def assertNoDecay(
    algorithm: CypherReplanAlgorithm,
    initialThreshold: Double,
    targetThreshold: Double,
    initialInterval: Long,
    targetInterval: Long
  ): Unit = {
    val divergence = StatsDivergenceCalculator.divergenceCalculatorFor(
      algorithm,
      initialThreshold,
      targetThreshold,
      initialInterval,
      targetInterval
    )
    assertNoDecay(algorithm, divergence, initialThreshold, initialInterval)
  }

  private def assertNoDecay(
    algorithm: CypherReplanAlgorithm,
    divergence: StatsDivergenceCalculator,
    threshold: Double,
    interval: Long
  ): Unit = {
    withClue(s"For decay algorithm '$algorithm': ") {
      divergence.decay(0) should be(threshold)
      divergence.decay(interval / 2) should be(threshold)
      divergence.decay(interval) should be(threshold)
      divergence.decay(interval * 2) should be(threshold)
    }
  }

  private def assertDecaysMakeSense(
    algorithm: CypherReplanAlgorithm,
    initialThreshold: Double,
    targetThreshold: Double,
    initialInterval: Long,
    targetInterval: Long
  ): Unit = {
    withClue("Testing intervals that differ by only 1 is not supported in this test:") {
      targetInterval should be >= initialInterval + 2
    }
    withClue(s"For decay algorithm '$algorithm': ") {
      val divergence = StatsDivergenceCalculator.divergenceCalculatorFor(
        algorithm,
        initialThreshold,
        targetThreshold,
        initialInterval,
        targetInterval
      )
      divergence.decay(initialInterval) should be(initialThreshold +- marginOfError)
      val decayed = divergence.decay((initialInterval + targetInterval) / 2)
      decayed should be > targetThreshold
      decayed should be < initialThreshold
      divergence.decay(targetInterval) should be(targetThreshold +- marginOfError)
      val furtherDecayed = divergence.decay(targetInterval * 2)
      furtherDecayed should be < targetThreshold
      furtherDecayed should be >= 0.0
    }
  }
}

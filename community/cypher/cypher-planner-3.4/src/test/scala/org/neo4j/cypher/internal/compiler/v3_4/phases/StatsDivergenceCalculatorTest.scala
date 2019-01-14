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
package org.neo4j.cypher.internal.compiler.v3_4.phases

import org.neo4j.cypher.internal.compiler.v3_4.StatsDivergenceCalculator
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Settings

class StatsDivergenceCalculatorTest extends CypherFunSuite {
  val defaultInitialThreshold = GraphDatabaseSettings.query_statistics_divergence_threshold.getDefaultValue().toDouble
  val defaultTargetThreshold = GraphDatabaseSettings.query_statistics_divergence_target.getDefaultValue.toDouble
  val defaultInitialInterval = Settings.DURATION.apply(GraphDatabaseSettings.cypher_min_replan_interval.getDefaultValue).toMillis
  val defaultTargetInterval = Settings.DURATION.apply(GraphDatabaseSettings.cypher_replan_interval_target.getDefaultValue).toMillis
  val marginOfError = 0.0001

  test("Disabling decay should show no decay") {
    val divergence = StatsDivergenceCalculator.divergenceNoDecayCalculator(defaultInitialThreshold, defaultInitialInterval)
    assertNoDecay(StatsDivergenceCalculator.none, divergence, defaultInitialThreshold, defaultInitialInterval)
  }

  test("Using algorithm 'none' decay should show no decay") {
    assertNoDecay(StatsDivergenceCalculator.none, defaultInitialThreshold, defaultTargetThreshold, defaultInitialInterval, defaultTargetInterval)
  }

  test("Default values should make sense") {
    assertDecaysMakeSense(Settings.DEFAULT, defaultInitialThreshold, defaultTargetThreshold, defaultInitialInterval, defaultTargetInterval)
  }

  test("Default values should make sense with inverse decay") {
    assertDecaysMakeSense(StatsDivergenceCalculator.inverse, defaultInitialThreshold, defaultTargetThreshold, defaultInitialInterval, defaultTargetInterval)
  }

  test("Default values should make sense with exponential decay") {
    assertDecaysMakeSense(StatsDivergenceCalculator.exponential, defaultInitialThreshold, defaultTargetThreshold, defaultInitialInterval, defaultTargetInterval)
  }

  test("Equal threshold should disable decay") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertNoDecay(name, 0.5, 0.5, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Increasing threshold should disable decay") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertNoDecay(name, 0.5, 0.75, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Target threshold of zero should disable decay") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertNoDecay(name, 0.5, 0.0, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Small target threshold should work") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertDecaysMakeSense(name, 0.5, 0.001, defaultInitialInterval, defaultTargetInterval)
    }
  }

  test("Not changing time should disable decay") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertNoDecay(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 1000)
    }
  }

  test("Going back in time should disable decay") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertNoDecay(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 500)
    }
  }

  test("Small time interval should work") {
    Seq(StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      assertDecaysMakeSense(name, defaultInitialThreshold, defaultTargetThreshold, 1000, 1002)
    }
  }

  private def assertNoDecay(name: String, initialThreshold: Double, targetThreshold: Double, initialInterval: Long, targetInterval: Long): Unit = {
    val divergence = StatsDivergenceCalculator.divergenceCalculatorFor(name, initialThreshold, targetThreshold, initialInterval, targetInterval)
    assertNoDecay(name, divergence, initialThreshold, initialInterval)
  }

  private def assertNoDecay(name: String, divergence: StatsDivergenceCalculator, threshold: Double, interval: Long): Unit = {
    withClue(s"For decay algorithm '$name': ") {
      divergence.decay(0) should be(threshold)
      divergence.decay(interval / 2) should be(threshold)
      divergence.decay(interval) should be(threshold)
      divergence.decay(interval * 2) should be(threshold)
    }
  }

  private def assertDecaysMakeSense(name: String, initialThreshold: Double, targetThreshold: Double, initialInterval: Long, targetInterval: Long): Unit = {
    withClue("Testing intervals that differ by only 1 is not supported in this test:") {
      targetInterval should be >= initialInterval + 2
    }
    withClue(s"For decay algorithm '$name': ") {
      val divergence = StatsDivergenceCalculator.divergenceCalculatorFor(name, initialThreshold, targetThreshold, initialInterval, targetInterval)
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

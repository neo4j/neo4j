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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compiler.v3_4.{NeedsReplan, FineToReuse, StatsDivergenceCalculator}
import org.neo4j.cypher.internal.planner.v3_4.spi.{GraphStatistics, GraphStatisticsSnapshot, NodesWithLabelCardinality}
import org.neo4j.cypher.internal.util.v3_4.LabelId
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.time.Clocks

class PlanFingerprintReferenceTest extends CypherFunSuite {

  test("should be stale if statistics increase enough to pass the threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.1, 0.05, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(6.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(2, SECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(42), stats)

        cacheCheck shouldBe a[NeedsReplan]
      }
    }
  }

  test("should be stale if statistics decrease enough to pass the threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.1, 0.05, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(4.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(2, SECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(42), stats)

        cacheCheck shouldBe a[NeedsReplan]
      }
    }
  }

  test("should not be stale if stats have increased but not enough to pass the threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(6.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(2, SECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(42), stats)

        cacheCheck shouldBe FineToReuse
      }
    }
  }

  test("should not be stale if stats have decreased but not enough to pass the threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(4.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(2, SECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(42), stats)

        cacheCheck shouldBe FineToReuse
      }
    }
  }

  test("should not be stale if txId didn't change") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        // Create valid calculator, but test that it is never used
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        // even with sufficient stats change we will remain stale
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(15.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(2, SECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(17), stats)

        cacheCheck shouldBe FineToReuse
      }
    }
  }

  test("should not be stale if life time has not expired") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        // Create valid calculator, but test that it is never used
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        // even with sufficient stats change we will remain stale
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(15.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        clock.forward(500, MILLISECONDS)

        val cacheCheck = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint).checkPlanReusability(transactionIdSupplier(42), stats)

        cacheCheck shouldBe FineToReuse
      }
    }
  }

  test("should update the timestamp if the life time is expired but transaction has not changed") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        // Create valid calculator, but test that it is never used
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        // even with sufficient stats change we will remain stale
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(15.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        val reference = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint)

        clock.forward(2, SECONDS)
        reference.checkPlanReusability(transactionIdSupplier(17), stats) shouldBe FineToReuse

        clock.forward(500, MILLISECONDS)
        reference.checkPlanReusability(transactionIdSupplier(23), stats) shouldBe FineToReuse
      }
    }
  }

  test("should update the timestamp and the txId if the life time is expired the txId is old but stats has not changed over the threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.1, 0.05, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        val reference = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint)

        clock.forward(2, SECONDS)
        reference.checkPlanReusability(transactionIdSupplier(23), stats) shouldBe FineToReuse

        clock.forward(2, SECONDS)
        reference.checkPlanReusability(transactionIdSupplier(23), stats) shouldBe FineToReuse
      }
    }
  }

  test("should be stale if statistics increase enough to pass the decayed threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(6.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        val reference = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint)

        clock.forward(2, SECONDS)
        reference.checkPlanReusability(transactionIdSupplier(23), stats) shouldBe FineToReuse

        clock.forward(100, SECONDS)
        val result = reference.checkPlanReusability(transactionIdSupplier(73), stats)
        if (name == StatsDivergenceCalculator.none)
          result shouldBe FineToReuse
        else
          result shouldBe a[NeedsReplan]
      }
    }
  }

  test("should be stale if statistics decrease enough to pass the decayed threshold") {
    Seq(StatsDivergenceCalculator.none, StatsDivergenceCalculator.inverse, StatsDivergenceCalculator.exponential).foreach { name =>
      withClue(s"For decay algorithm '$name': ") {
        val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
        val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(name, 0.5, 0.1, 1000, 100000)
        val clock = Clocks.fakeClock()
        val stats = mock[GraphStatistics]
        when(stats.nodesWithLabelCardinality(label(21))).thenReturn(4.0)
        val fingerprint = PlanFingerprint(clock.millis(), 17, snapshot)

        val reference = new PlanFingerprintReference(clock, divergenceCalculator, fingerprint)

        clock.forward(2, SECONDS)
        reference.checkPlanReusability(transactionIdSupplier(23), stats) shouldBe FineToReuse

        clock.forward(100, SECONDS)
        val result = reference.checkPlanReusability(transactionIdSupplier(73), stats)
        if (name == StatsDivergenceCalculator.none)
          result shouldBe FineToReuse
        else
          result shouldBe a[NeedsReplan]
      }
    }
  }

  implicit def liftToOption[T](item: T): Option[T] = Option(item)
  def transactionIdSupplier[T](item: T): () => T = () => item
  def label(i: Int): LabelId = LabelId(i)
}

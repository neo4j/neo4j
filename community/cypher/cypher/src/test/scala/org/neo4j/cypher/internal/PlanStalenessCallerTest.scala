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
package org.neo4j.cypher.internal

import org.mockito.Mockito.when
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherReplanAlgorithm
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.GraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodesWithLabelCardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.time.Clocks
import org.neo4j.time.FakeClock

import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

import scala.language.implicitConversions

class PlanStalenessCallerTest extends CypherFunSuite {

  private val divergenceCalculators =
    Seq(CypherReplanAlgorithm.NONE, CypherReplanAlgorithm.INVERSE, CypherReplanAlgorithm.EXPONENTIAL)

  test("should be stale if statistics increase enough to pass the threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.1, 0.05, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 6.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(42), not_used, null)

      clock.forward(2, SECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe a[Stale]
    }
  }

  test("should be stale if statistics decrease enough to pass the threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.1, 0.05, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 4.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(42), not_used, null)

      clock.forward(2, SECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe a[Stale]
    }
  }

  test("should not be stale if stats have increased but not enough to pass the threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 6.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(42), not_used, null)

      clock.forward(2, SECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale
    }
  }

  test("should not be stale if stats have decreased but not enough to pass the threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 4.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(42), not_used, null)

      clock.forward(2, SECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale
    }
  }

  test("should not be stale if txId didn't change") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))

      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      // even with sufficient stats change we will remain stale
      val stats: GraphStatistics = nodesWithLabelCardinality(21, 15.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(17), not_used, null)

      clock.forward(2, SECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale
    }
  }

  test("should not be stale if life time has not expired") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      // even with sufficient stats change we will remain stale
      val stats: GraphStatistics = nodesWithLabelCardinality(21, 15.0)
      val planStalenessCaller =
        new DefaultPlanStalenessCaller(clock, divergenceCalculator, TransactionIdSupplier(42), not_used, null)

      clock.forward(500, MILLISECONDS)

      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale
    }
  }

  test("should not update the timestamp if the life time is expired but transaction has not changed") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      // even with sufficient stats change we will remain stale
      val stats: GraphStatistics = nodesWithLabelCardinality(21, 15.0)
      val idSupplier = TransactionIdSupplier(17)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      clock.forward(2, SECONDS)
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale

      clock.forward(500, MILLISECONDS)
      idSupplier.id = 23
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe a[Stale]
    }
  }

  test("should only update timestamp when at least minReplanInterval time has passed") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 15.0)
      val idSupplier = TransactionIdSupplier(17)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      clock.forward(500, MILLISECONDS)
      idSupplier.id = 23
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale

      clock.forward(500, MILLISECONDS)
      idSupplier.id = 24
      // even though we checked staleness once after 500 ms, the total passed time is now 1000 ms and our stalenessCaller should therefore do the calculation
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe a[Stale]
    }
  }

  test(
    "should update the timestamp and the txId if the life time is expired the txId is old but stats has not changed over the threshold"
  ) {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.1, 0.05, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 5.0)
      val idSupplier = TransactionIdSupplier(23)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      clock.forward(2, SECONDS)
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale

      clock.forward(2, SECONDS)
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale
    }
  }

  test("should be stale if statistics increase enough to pass the decayed threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 6.0)
      val idSupplier = TransactionIdSupplier(23)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      clock.forward(2, SECONDS)
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale

      clock.forward(100, SECONDS)
      idSupplier.id = 73
      val result = planStalenessCaller.staleness(fingerprintRef, stats, -1)
      if (algorithm == CypherReplanAlgorithm.NONE)
        result shouldBe NotStale
      else
        result shouldBe a[Stale]
    }
  }

  test("should be stale if statistics decrease enough to pass the decayed threshold") {
    testAll { (algorithm, clock) =>
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef = new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, None))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 4.0)
      val idSupplier = TransactionIdSupplier(23)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      clock.forward(2, SECONDS)
      planStalenessCaller.staleness(fingerprintRef, stats, -1) shouldBe NotStale

      clock.forward(100, SECONDS)
      idSupplier.id = 73
      val result = planStalenessCaller.staleness(fingerprintRef, stats, -1)
      if (algorithm == CypherReplanAlgorithm.NONE)
        result shouldBe NotStale
      else
        result shouldBe a[Stale]
    }
  }

  test("should be stale if procedureSignatureVersion changes") {
    testAll { (algorithm, clock) =>
      val procedureSignatureVersion = 100
      val staleProcedureSignatureVersion = -procedureSignatureVersion
      val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
      val fingerprintRef =
        new PlanFingerprintReference(PlanFingerprint(clock.millis(), 17, snapshot, Some(procedureSignatureVersion)))
      val divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(algorithm, 0.5, 0.1, 1000, 100000)

      val stats: GraphStatistics = nodesWithLabelCardinality(21, 15.0)
      val idSupplier = TransactionIdSupplier(17)
      val planStalenessCaller = new DefaultPlanStalenessCaller(clock, divergenceCalculator, idSupplier, not_used, null)

      idSupplier.id = 23
      planStalenessCaller.staleness(fingerprintRef, stats, procedureSignatureVersion) shouldBe NotStale
      planStalenessCaller.staleness(fingerprintRef, stats, staleProcedureSignatureVersion) shouldBe a[Stale]
    }
  }

  def testAll(f: (CypherReplanAlgorithm, FakeClock) => Unit): Unit = {
    for (algorithm <- divergenceCalculators)
      withClue(s"For divergence calculator '$algorithm': ") { f(algorithm, Clocks.fakeClock()) }
  }

  private def nodesWithLabelCardinality(labelId: Int, cardinality: Double): GraphStatistics = {
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(labelId))).thenReturn(cardinality)
    stats
  }

  implicit def liftToOption[T](item: T): Option[T] = Option(item)

  case class TransactionIdSupplier(var id: Long) extends (() => Long) {
    override def apply(): Long = id
  }

  private def label(i: Int): LabelId = LabelId(i)
  private def not_used(plan: ExecutableQuery, tc: TransactionalContext) = FineToReuse
}

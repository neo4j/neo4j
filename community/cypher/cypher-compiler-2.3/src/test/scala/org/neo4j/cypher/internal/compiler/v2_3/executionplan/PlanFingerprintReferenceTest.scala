/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compiler.v2_3.spi.{GraphStatistics, GraphStatisticsSnapshot, NodesWithLabelCardinality}
import org.neo4j.cypher.internal.frontend.v2_3.LabelId
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.helpers.FakeClock

class PlanFingerprintReferenceTest extends CypherFunSuite {

  test("should be stale if all properties are out of date") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 4.0))
    val ttl = 1000l
    val threshold = 0.0
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    clock.forward(2, SECONDS)

    val stale = new PlanFingerprintReference(clock, ttl, threshold, fingerprint).isStale(->(42), stats)

    stale shouldBe true
  }

  test("should not be stale if stats are not diverged over the threshold") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
    val ttl = 1000l
    val threshold = 0.5
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    clock.forward(2, SECONDS)

    val stale = new PlanFingerprintReference(clock, ttl, threshold, fingerprint).isStale(->(42), stats)

    stale shouldBe false
  }

  test("should not be stale if txId didn't change") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
    val ttl = 1000l
    val threshold = 0.0
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    clock.forward(2, SECONDS)

    val stale = new PlanFingerprintReference(clock, ttl, threshold, fingerprint).isStale(->(17), stats)

    stale shouldBe false
  }

  test("should not be stale if life time has not expired") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
    val ttl = 1000l
    val threshold = 0.0
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    clock.forward(500, MILLISECONDS)

    val stale = new PlanFingerprintReference(clock, ttl, threshold, fingerprint).isStale(->(42), stats)

    stale shouldBe false
  }

  test("should update the timestamp if the life time is expired but transaction has not changed") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
    val ttl = 1000l
    val threshold = 0.0
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    val reference = new PlanFingerprintReference(clock, ttl, threshold, fingerprint)

    clock.forward(2, SECONDS)
    reference.isStale(->(17), stats) shouldBe false

    clock.forward(500, MILLISECONDS)
    reference.isStale(->(23), stats) shouldBe false
  }

  test("should update the timestamp and the txId if the life time is expired the txId is old but stats has not changed over the threshold") {
    val snapshot = GraphStatisticsSnapshot(Map(NodesWithLabelCardinality(label(21)) -> 5.0))
    val ttl = 1000l
    val threshold = 0.1
    val clock = new FakeClock
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(label(21))).thenReturn(5.0)
    val fingerprint = PlanFingerprint(clock.currentTimeMillis(), 17, snapshot)

    val reference = new PlanFingerprintReference(clock, ttl, threshold, fingerprint)

    clock.forward(2, SECONDS)
    reference.isStale(->(23), stats) shouldBe false

    clock.forward(2, SECONDS)
    reference.isStale(->(23), stats) shouldBe false
  }

  implicit def liftToOption[T](item: T): Option[T] = Option(item)
  def ->[T](item: T): () => T = () => item
  def label(i: Int): LabelId = LabelId(i)
}

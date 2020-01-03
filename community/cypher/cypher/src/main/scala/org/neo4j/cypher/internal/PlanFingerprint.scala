/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, GraphStatisticsSnapshot, InstrumentedGraphStatistics}

case class PlanFingerprint(creationTimeMillis: Long, lastCheckTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot) {
  if (snapshot.statsValues.isEmpty) {
    throw new IllegalArgumentException("Cannot create plan fingerprint with empty graph statistics snapshot")
  }
}

object PlanFingerprint {
  def apply(creationTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot): PlanFingerprint =
    PlanFingerprint(creationTimeMillis, creationTimeMillis, txId, snapshot)

  def take(clock: Clock, txIdProvider: () => Long, graphStatistics: InstrumentedGraphStatistics): PlanFingerprint =
    PlanFingerprint(clock.millis(), txIdProvider(), graphStatistics.snapshot.freeze)
}

class PlanFingerprintReference(var fingerprint: PlanFingerprint)

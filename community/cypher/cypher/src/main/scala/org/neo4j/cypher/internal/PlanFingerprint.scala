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

import org.neo4j.cypher.internal.planner.spi.GraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics

import java.time.Clock

/**
 *
 * @param creationTimeMillis  time of creation
 * @param lastCheckTimeMillis time this fingerprint was last checked
 * @param lastCommittedTxId   highest seen committed transaction id when this fingerprint was created.
 * @param snapshot            a snapshot of the statistics used to compute the plan in a cache
 * @param procedureSignatureVersion    the signature version that was used to resolve procedures (if any).
 */
case class PlanFingerprint(
  creationTimeMillis: Long,
  lastCheckTimeMillis: Long,
  lastCommittedTxId: Long,
  snapshot: GraphStatisticsSnapshot,
  procedureSignatureVersion: Option[Long]
) {

  if (snapshot.statsValues.isEmpty) {
    throw new IllegalArgumentException("Cannot create plan fingerprint with empty graph statistics snapshot")
  }
}

object PlanFingerprint {

  def apply(
    creationTimeMillis: Long,
    txId: Long,
    snapshot: GraphStatisticsSnapshot,
    procedureSignatureVersion: Option[Long]
  ): PlanFingerprint =
    PlanFingerprint(creationTimeMillis, creationTimeMillis, txId, snapshot, procedureSignatureVersion)

  def take(
    clock: Clock,
    lastCommittedTxIdProvider: () => Long,
    graphStatistics: InstrumentedGraphStatistics,
    procedureSignatureVersion: Option[Long]
  ): PlanFingerprint =
    PlanFingerprint(
      clock.millis(),
      lastCommittedTxIdProvider(),
      graphStatistics.snapshot.freeze,
      procedureSignatureVersion
    )
}

class PlanFingerprintReference(var fingerprint: PlanFingerprint)

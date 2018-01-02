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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, GraphStatisticsSnapshot}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CacheCheckResult, StatsDivergenceCalculator}

case class PlanFingerprint(creationTimeMillis: Long, lastCheckTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot)

class PlanFingerprintReference(clock: Clock, divergence: StatsDivergenceCalculator,
                               private var fingerprint: Option[PlanFingerprint]) {

  def isStale(lastCommittedTxId: () => Long, statistics: GraphStatistics): CacheCheckResult = {
    fingerprint.fold(CacheCheckResult.empty) { f =>
      lazy val currentTimeMillis = clock.millis()
      lazy val currentTxId = lastCommittedTxId()

      CacheCheckResult(divergence.shouldCheck(currentTimeMillis, f.lastCheckTimeMillis) &&
        check(currentTxId != f.txId,
          () => {
            fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis))
          }) &&
        check(f.snapshot.diverges(f.snapshot.recompute(statistics), divergence.decay(currentTimeMillis - f.creationTimeMillis)),
          () => {
            fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis, txId = currentTxId))
          }),
        ((currentTimeMillis - f.creationTimeMillis)/1000).toInt)
    }
  }

  private def check(test: => Boolean, ifFalse: () => Unit ) = if (test) { true } else { ifFalse() ; false }
}

object PlanFingerprint {
  def apply(creationTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot): PlanFingerprint =
    PlanFingerprint(creationTimeMillis, creationTimeMillis, txId, snapshot)
}

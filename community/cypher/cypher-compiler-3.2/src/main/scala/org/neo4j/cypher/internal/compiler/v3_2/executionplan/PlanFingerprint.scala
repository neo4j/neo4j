/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, GraphStatisticsSnapshot}

case class PlanFingerprint(creationTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot)

class PlanFingerprintReference(clock: Clock, minimalTimeToLive: Long, statsDivergenceThreshold : Double,
                               private var fingerprint: Option[PlanFingerprint]) {

  def isStale(lastCommittedTxId: () => Long, statistics: GraphStatistics): Boolean = {
    fingerprint.fold({println("Replanning: NO  - No fingerprint"); false}) { f =>
      lazy val currentTimeMillis = clock.millis()
      lazy val currentTxId = lastCommittedTxId()

      println(s"Replanning: Creation:${f.creationTimeMillis} TTL:$minimalTimeToLive Current:$currentTimeMillis currentTx:$currentTxId")

      if(!(f.creationTimeMillis + minimalTimeToLive <= currentTimeMillis)) {
        println("Replanning: NO  - Shorter than minimal TTL")
        return false
      }
      if(!(check(currentTxId != f.txId,
        () => { fingerprint = Some(f.copy(creationTimeMillis = currentTimeMillis)) }))) {
        println(s"Replanning: NO  - Same txID. Setting CreationTime to $currentTimeMillis")
        return false
      }
      if(!(check(f.snapshot.diverges(f.snapshot.recompute(statistics), statsDivergenceThreshold),
        () => { fingerprint = Some(f.copy(creationTimeMillis = currentTimeMillis, txId = currentTxId)) }))) {
        println(s"Replanning: NO  - Statistics don't diverge. Setting CreationTime to $currentTimeMillis. Setting txID to $currentTxId")
        return false
      }
      println("Replanning: YES")
      true
    }
  }

  private def check(test: => Boolean, ifFalse: () => Unit ) = if (test) { true } else { ifFalse() ; false }
}

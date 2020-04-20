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

import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.spi.TransactionBoundGraphStatistics
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.Log

/**
 * Decides whether a plan is stale or not, depending on it's fingerprint.
 *
 * @param clock Clock for measuring elapsed time.
 * @param divergenceCalculator Computes is the plan i stale depending on changes in the underlying
 *                   statistics, and how much time has passed.
 * @param lastCommittedTxIdProvider Reports the id of the latest committed transaction.
 */
class DefaultPlanStalenessCaller[EXECUTABLE_QUERY](clock: Clock,
                                                   divergenceCalculator: StatsDivergenceCalculator,
                                                   lastCommittedTxIdProvider: () => Long,
                                                   reusabilityInfo: (EXECUTABLE_QUERY, TransactionalContext) => ReusabilityState,
                                                   log: Log) extends PlanStalenessCaller[EXECUTABLE_QUERY] {

  override def staleness(transactionalContext: TransactionalContext,
                         cachedExecutableQuery: EXECUTABLE_QUERY): Staleness = {
    val reusability = reusabilityInfo(cachedExecutableQuery, transactionalContext)
    reusability match {
      case MaybeReusable(ref) =>
        val ktx = transactionalContext.kernelTransaction()
        staleness(ref, TransactionBoundGraphStatistics(ktx.dataRead, ktx.schemaRead, log))

      case FineToReuse => NotStale
      case NeedsReplan(x) => Stale(x, None)
    }
  }

  private[internal] def staleness(ref: PlanFingerprintReference, statistics: => GraphStatistics): Staleness = {
    val f = ref.fingerprint
    lazy val currentTimeMillis = clock.millis()
    // TODO: remove this tx-id stuff.
    // Cannot understand why that would work? The last committed id cannot be this tx,
    // because for us to plan a query this tx has to be open, e.g. not committed.
    lazy val currentTxId = lastCommittedTxIdProvider()

    if (divergenceCalculator.shouldCheck(currentTimeMillis, f.lastCheckTimeMillis) && currentTxId != f.txId) {
      //check if we have diverged?
      val threshold = divergenceCalculator.decay(currentTimeMillis - f.creationTimeMillis)
      val divergence = f.snapshot.diverges(f.snapshot.recompute(statistics))
      if (divergence.divergence > threshold) {
        Stale(((currentTimeMillis - f.creationTimeMillis) / 1000).toInt,
          Option(s"${divergence.key} changed from ${divergence.before} to ${divergence.after}, " +
            s"which is a divergence of ${divergence.divergence} which is greater than " +
            s"threshold $threshold"))
      } else {
        ref.fingerprint = f.copy(lastCheckTimeMillis = currentTimeMillis, txId = currentTxId)
        NotStale
      }
    } else {
      ref.fingerprint = f.copy(lastCheckTimeMillis = currentTimeMillis)
      NotStale
    }
  }
}

sealed trait ReusabilityState
case class NeedsReplan(secondsSincePlan: Int) extends ReusabilityState
case class MaybeReusable(fingerprint: PlanFingerprintReference) extends ReusabilityState
case object FineToReuse extends ReusabilityState

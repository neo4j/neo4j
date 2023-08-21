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

import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.spi.TransactionBoundGraphStatistics
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.InternalLog

import java.time.Clock

/**
 * Decides whether a plan is stale or not, depending on it's fingerprint.
 *
 * @param clock                     Clock for measuring elapsed time.
 * @param divergenceCalculator      Computes is the plan i stale depending on changes in the underlying
 *                                  statistics, and how much time has passed.
 * @param lastCommittedTxIdProvider Reports the id of the latest committed transaction.
 */
class DefaultPlanStalenessCaller[EXECUTABLE_QUERY](
  clock: Clock,
  divergenceCalculator: StatsDivergenceCalculator,
  lastCommittedTxIdProvider: () => Long,
  reusabilityInfo: (EXECUTABLE_QUERY, TransactionalContext) => ReusabilityState,
  log: InternalLog
) extends PlanStalenessCaller[EXECUTABLE_QUERY] {

  override def staleness(
    transactionalContext: TransactionalContext,
    cachedExecutableQuery: EXECUTABLE_QUERY
  ): Staleness = {

    val reusability = reusabilityInfo(cachedExecutableQuery, transactionalContext)
    reusability match {
      case MaybeReusable(ref) =>
        val ktx = transactionalContext.kernelTransaction()
        staleness(
          ref,
          TransactionBoundGraphStatistics(ktx.dataRead, ktx.schemaRead, log),
          ktx.procedures.signatureVersion
        )

      case FineToReuse    => NotStale
      case NeedsReplan(x) => Stale(x, None)
    }
  }

  private[internal] def staleness(
    ref: PlanFingerprintReference,
    statistics: => GraphStatistics,
    txnProcedureSignatureVersion: Long
  ): Staleness = {
    val f = ref.fingerprint
    lazy val currentTimeMillis = clock.millis()
    lazy val lastCommittedTxId = lastCommittedTxIdProvider()
    lazy val secondsSincePlan = ((currentTimeMillis - f.creationTimeMillis) / 1000).toInt

    if (f.procedureSignatureVersion.exists(_ != txnProcedureSignatureVersion)) {
      // If we have resolved a procedure, we need to verify that it was resolved
      // with the same signatureVersion as the current transaction uses.
      Stale(secondsSincePlan, Some("Procedure or function signature have been modified"))
    } else if (
      divergenceCalculator.shouldCheck(currentTimeMillis, f.lastCheckTimeMillis) &&
      lastCommittedTxId != f.lastCommittedTxId
    ) {
      // check if we have diverged?
      val threshold = divergenceCalculator.decay(currentTimeMillis - f.creationTimeMillis)
      val divergence = f.snapshot.diverges(f.snapshot.recompute(statistics))
      if (divergence.divergence > threshold) {
        Stale(
          secondsSincePlan,
          Option(s"${divergence.key} changed from ${divergence.before} to ${divergence.after}, " +
            s"which is a divergence of ${divergence.divergence} which is greater than " +
            s"threshold $threshold")
        )
      } else {
        ref.fingerprint = f.copy(lastCheckTimeMillis = currentTimeMillis, lastCommittedTxId = lastCommittedTxId)
        NotStale
      }
    } else {
      NotStale
    }
  }
}

sealed trait ReusabilityState
case class NeedsReplan(secondsSincePlan: Int) extends ReusabilityState
case class MaybeReusable(fingerprint: PlanFingerprintReference) extends ReusabilityState
case object FineToReuse extends ReusabilityState

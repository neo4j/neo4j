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

import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundGraphStatistics, TransactionalContextWrapper}
import org.neo4j.kernel.impl.query.TransactionalContext

/**
  * Decides whether a plan is stale or not, depending on it's fingerprint.
  *
  * @param clock Clock for measuring elapsed time.
  * @param divergence Computes is the plan i stale depending on changes in the underlying
  *                   statistics, and how much time has passed.
  * @param lastCommittedTxIdProvider Reports the id of the latest committed transaction.
  */
class PlanStalenessCaller[EXECUTABLE_QUERY](clock: Clock,
                                            divergence: StatsDivergenceCalculator,
                                            lastCommittedTxIdProvider: () => Long,
                                            reusabilityInfo: (EXECUTABLE_QUERY, TransactionalContext) => ReusabilityState) {

  def staleness(transactionalContext: TransactionalContext,
                cachedExecutableQuery: EXECUTABLE_QUERY): Staleness = {
    val reusability = reusabilityInfo(cachedExecutableQuery, transactionalContext)
    reusability match {
      case MaybeReusable(ref) =>
        val ktx = transactionalContext.kernelTransaction()
        staleness(ref, TransactionBoundGraphStatistics(ktx.dataRead, ktx.schemaRead))

      case FineToReuse => NotStale
      case NeedsReplan(x) => Stale(x)
    }
  }

  def staleness(ref: PlanFingerprintReference, statistics: => GraphStatistics): Staleness = {
    val f = ref.fingerprint
    lazy val currentTimeMillis = clock.millis()
    // TODO: remove this tx-id stuff.
    // Cannot understand why that would work? The last committed id cannot be this tx,
    // because for us to plan a query this tx has to be open, e.g. not committed.
    lazy val currentTxId = lastCommittedTxIdProvider()

    val stale = divergence.shouldCheck(currentTimeMillis, f.lastCheckTimeMillis) &&
      check(currentTxId != f.txId,
            () => {
              ref.fingerprint = f.copy(lastCheckTimeMillis = currentTimeMillis)
            }) &&
      check(f.snapshot.diverges(f.snapshot.recompute(statistics), divergence.decay(currentTimeMillis - f.creationTimeMillis)),
            () => {
              ref.fingerprint = f.copy(lastCheckTimeMillis = currentTimeMillis, txId = currentTxId)
            })

    if(stale) {
      val secondsSinceReplan = ((currentTimeMillis - f.creationTimeMillis) / 1000).toInt
      Stale(secondsSinceReplan)
    } else
      NotStale
  }

  private def check(test: => Boolean, ifFalse: () => Unit ) = if (test) { true } else { ifFalse() ; false }
}

sealed trait ReusabilityState
case class NeedsReplan(secondsSincePlan: Int) extends ReusabilityState
case class MaybeReusable(fingerprint: PlanFingerprintReference) extends ReusabilityState
case object FineToReuse extends ReusabilityState

sealed trait Staleness
case object NotStale extends Staleness
case class Stale(secondsSincePlan: Int) extends Staleness

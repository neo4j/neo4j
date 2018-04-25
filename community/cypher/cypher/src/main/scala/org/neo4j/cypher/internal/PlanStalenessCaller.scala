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
                                            plan: EXECUTABLE_QUERY => ExecutionPlan) {

  def staleness(transactionalContext: TransactionalContext,
                cachedExecutableQuery: EXECUTABLE_QUERY): Staleness = {
    val reusability = plan(cachedExecutableQuery).reusabilityInfo(lastCommittedTxIdProvider, TransactionalContextWrapper(transactionalContext))
    reusability match {
      case MaybeReusable(ref) if ref.fingerprint.nonEmpty =>
        val ktx = transactionalContext.kernelTransaction()
        staleness(ref, TransactionBoundGraphStatistics(ktx.dataRead, ktx.schemaRead))

      case FineToReuse => NotStale
      case NeedsReplan(x) => Stale(x)
    }
  }

  def staleness(ref: PlanFingerprintReference, statistics: => GraphStatistics): Staleness = {
    val f = ref.fingerprint.get
    lazy val currentTimeMillis = clock.millis()
    // TODO: remove this tx-id stuff.
    // Cannot understand why that would work? The last committed id cannot be this tx,
    // because for us to plan a query this tx has to be open, e.g. not committed.
    lazy val currentTxId = lastCommittedTxIdProvider()

    val stale = divergence.shouldCheck(currentTimeMillis, f.lastCheckTimeMillis) &&
      check(currentTxId != f.txId,
            () => {
              ref.fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis))
            }) &&
      check(f.snapshot.diverges(f.snapshot.recompute(statistics), divergence.decay(currentTimeMillis - f.creationTimeMillis)),
            () => {
              ref.fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis, txId = currentTxId))
            })

    if(stale) {
      val secondsSinceReplan = ((currentTimeMillis - f.creationTimeMillis) / 1000).toInt
      Stale(secondsSinceReplan)
    } else
      NotStale
  }

  private def check(test: => Boolean, ifFalse: () => Unit ) = if (test) { true } else { ifFalse() ; false }
}

sealed trait ReusabilityInfo
case class NeedsReplan(secondsSincePlan: Int) extends ReusabilityInfo
case class MaybeReusable(fingerprint: PlanFingerprintReference) extends ReusabilityInfo
case object FineToReuse extends ReusabilityInfo

sealed trait Staleness
case object NotStale extends Staleness
case class Stale(secondsSincePlan: Int) extends Staleness

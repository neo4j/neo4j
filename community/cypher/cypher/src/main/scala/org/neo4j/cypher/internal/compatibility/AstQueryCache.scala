package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.RuntimeName
import org.neo4j.cypher.internal.{CacheTracer, NewQueryCache, PlanStalenessCaller, ReusabilityInfo}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_5.StatsDivergenceCalculator
import org.neo4j.cypher.internal.frontend.v3_5.PlannerName
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundGraphStatistics
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, QueryContext}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

class AstQueryCache[STATEMENT <: AnyRef](override val maximumSize: Int,
                                         override val tracer: CacheTracer[STATEMENT],
                                         clock: Clock,
                                         divergence: StatsDivergenceCalculator,
                                         lastCommittedTxIdProvider: () => Long
) extends NewQueryCache[STATEMENT, ExecutionPlan](maximumSize,
                                                  AstQueryCache.stalenessCaller(clock, divergence, lastCommittedTxIdProvider),
                                                  tracer,
                                                  AstQueryCache.BEING_RECOMPILED) {

}

object AstQueryCache {
  val BEING_RECOMPILED: ExecutionPlan =
    new ExecutionPlan {
      override def plannerUsed: PlannerName = ???
      override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = ???
      override def runtimeUsed: RuntimeName = ???
      override def isPeriodicCommit: Boolean = ???
      override def checkPlanResusability(lastTxId: () => Long, statistics: GraphStatistics): ReusabilityInfo = ???
    }

  def stalenessCaller(clock: Clock,
                      divergence: StatsDivergenceCalculator,
                      txIdProvider: () => Long): PlanStalenessCaller[ExecutionPlan] = {
    def planReusabilitiy(executionPlan: ExecutionPlan,
                         transactionalContext: TransactionalContext): ReusabilityInfo =
      executionPlan.checkPlanResusability(txIdProvider, TransactionBoundGraphStatistics(transactionalContext))

    new PlanStalenessCaller[ExecutionPlan](clock, divergence, txIdProvider, planReusabilitiy)
  }
}

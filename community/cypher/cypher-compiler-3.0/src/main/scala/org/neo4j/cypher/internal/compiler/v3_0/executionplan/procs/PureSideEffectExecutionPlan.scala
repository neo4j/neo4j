package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutionPlan, InternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{GraphStatistics, QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionMode, ExplainExecutionResult, ExplainMode, PlannerName, ProcedurePlannerName, ProcedureRuntimeName, RuntimeName, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.graphdb.QueryExecutionType.QueryType

/**
  * Execution plan for performing pure side-effects, i.e. returning no data to the user.
  * @param name A name of the side-effect
  * @param queryType The type of the query
  * @param sideEffect The actual side-effect to be performed
  */
case class PureSideEffectExecutionPlan(name: String, queryType: QueryType, sideEffect: (QueryContext => Unit))
  extends ExecutionPlan {

  override def run(ctx: QueryContext, planType: ExecutionMode,
                   params: Map[String, Any]): InternalExecutionResult = {
    val countingCtx = new UpdateCountingQueryContext(ctx)
    val taskCloser = new TaskCloser
    taskCloser.addTask(countingCtx.close)
    if (planType == ExplainMode) {
      //close all statements
      taskCloser.close(success = true)
      new ExplainExecutionResult(List.empty, description, queryType, Set.empty)
    } else
      sideEffect(countingCtx)
      taskCloser.close(success = true)
      PureSideEffectInternalExecutionResult(description, taskCloser, countingCtx, queryType)
  }

  private def description = PlanDescriptionImpl(new Id, name, NoChildren, Seq.empty, Set.empty)

  override def runtimeUsed: RuntimeName = ProcedureRuntimeName

  override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = false

  override def plannerUsed: PlannerName = ProcedurePlannerName

  override def notifications: Seq[InternalNotification] = Seq.empty

  override def isPeriodicCommit: Boolean = false
}

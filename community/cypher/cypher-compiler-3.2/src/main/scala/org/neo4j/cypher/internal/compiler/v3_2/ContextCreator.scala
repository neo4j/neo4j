package org.neo4j.cypher.internal.compiler.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors}

trait ContextCreator {
  def create(tracer: CompilationPhaseTracer,
             notificationLogger: InternalNotificationLogger,
             planContext: PlanContext,
             queryText: String,
             offset: Option[InputPosition]): CompilerContext
}

class CommunityContextCreator(monitors: Monitors,
                              createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                              typeConverter: RuntimeTypeConverter,
                              metricsFactory: MetricsFactory,
                              queryGraphSolver: QueryGraphSolver,
                              config: CypherCompilerConfiguration,
                              updateStrategy: UpdateStrategy,
                              clock: Clock) extends ContextCreator {
  override def create(tracer: CompilationPhaseTracer,
                      notificationLogger: InternalNotificationLogger,
                      planContext: PlanContext,
                      queryText: String,
                      offset: Option[InputPosition]): CompilerContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics)

    new CompilerContext(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference,
      monitors, metrics, queryGraphSolver, config, updateStrategy, clock)
  }

}

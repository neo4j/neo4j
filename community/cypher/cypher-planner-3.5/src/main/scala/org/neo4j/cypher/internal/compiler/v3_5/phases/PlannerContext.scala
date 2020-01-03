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
package org.neo4j.cypher.internal.compiler.v3_5.phases

import java.time.Clock

import org.neo4j.cypher.internal.v3_5.util.{CypherException, InputPosition}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{ExpressionEvaluator, Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.v3_5.frontend.phases.{BaseContext, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen

class PlannerContext(val exceptionCreator: (String, InputPosition) => CypherException,
                     val tracer: CompilationPhaseTracer,
                     val notificationLogger: InternalNotificationLogger,
                     val planContext: PlanContext,
                     val monitors: Monitors,
                     val metrics: Metrics,
                     val config: CypherPlannerConfiguration,
                     val queryGraphSolver: QueryGraphSolver,
                     val updateStrategy: UpdateStrategy,
                     val debugOptions: Set[String],
                     val clock: Clock,
                     val logicalPlanIdGen: IdGen) extends BaseContext {

  override def errorHandler =
    (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw exceptionCreator(e.msg, e.position))

}

object PlannerContextCreator extends ContextCreator[PlannerContext] {
  override def create(tracer: CompilationPhaseTracer,
                      notificationLogger: InternalNotificationLogger,
                      planContext: PlanContext,
                      queryText: String,
                      debugOptions: Set[String],
                      offset: Option[InputPosition],
                      monitors: Monitors,
                      metricsFactory: MetricsFactory,
                      queryGraphSolver: QueryGraphSolver,
                      config: CypherPlannerConfiguration,
                      updateStrategy: UpdateStrategy,
                      clock: Clock,
                      logicalPlanIdGen: IdGen,
                      evaluator: ExpressionEvaluator
                     ): PlannerContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics, evaluator, config)

    new PlannerContext(exceptionCreator, tracer, notificationLogger, planContext,
      monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen)
  }
}

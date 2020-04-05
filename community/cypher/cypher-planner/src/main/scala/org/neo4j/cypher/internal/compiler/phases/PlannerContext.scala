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
package org.neo4j.cypher.internal.compiler.phases

import java.time.Clock

import org.neo4j.cypher.internal.compiler.planner.logical.{ExpressionEvaluator, Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.{ContextCreator, CypherPlannerConfiguration, Neo4jCypherExceptionFactory, SyntaxExceptionCreator, UpdateStrategy}
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseContext, CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen
import org.neo4j.cypher.internal.v4_0.util.{CypherException, CypherExceptionFactory, InputPosition}
import org.neo4j.values.virtual.MapValue

class PlannerContext(val cypherExceptionFactory: CypherExceptionFactory,
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
                     val logicalPlanIdGen: IdGen,
                     val innerVariableNamer: InnerVariableNamer,
                     val params: MapValue) extends BaseContext {

  override def errorHandler: Seq[SemanticErrorDef] => Unit =
    (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw cypherExceptionFactory.syntaxException(e.msg, e.position))

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
                      evaluator: ExpressionEvaluator,
                      innerVariableNamer: InnerVariableNamer,
                      params: MapValue
                     ): PlannerContext = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics, evaluator, config)

    new PlannerContext(exceptionFactory, tracer, notificationLogger, planContext,
      monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen, innerVariableNamer, params)
  }
}

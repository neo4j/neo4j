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

import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.compiler.ContextCreator
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.UpdateStrategy
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.symbols.CypherType
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

  override val errorHandler: Seq[SemanticErrorDef] => Unit =
    SyntaxExceptionCreator.throwOnError(cypherExceptionFactory)

  def getParameterValueTypeMapping: Map[String, CypherType] = {
    ParameterValueTypeHelper.asCypherTypeMap(params)
  }

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

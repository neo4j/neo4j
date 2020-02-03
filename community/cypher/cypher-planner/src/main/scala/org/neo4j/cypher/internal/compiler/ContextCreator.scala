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
package org.neo4j.cypher.internal.compiler

import java.time.Clock

import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.values.virtual.MapValue

trait ContextCreator[Context <: BaseContext] {
  def create(tracer: CompilationPhaseTracer,
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
             params: MapValue ): Context
}

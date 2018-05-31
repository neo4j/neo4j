/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5

import java.time.Clock

import org.opencypher.v9_0.util.InputPosition
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{ExpressionEvaluator, MetricsFactory, QueryGraphSolver}
import org.opencypher.v9_0.frontend.phases.{BaseContext, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer
import org.opencypher.v9_0.util.attribution.IdGen

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
             evaluator: ExpressionEvaluator): Context
}

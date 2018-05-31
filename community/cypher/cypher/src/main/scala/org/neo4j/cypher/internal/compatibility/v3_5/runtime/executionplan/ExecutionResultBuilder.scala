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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import org.opencypher.v9_0.util.CypherException
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.RuntimeName
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, QueryContext}
import org.neo4j.values.virtual.MapValue

trait ExecutionResultBuilder {
  def setQueryContext(context: QueryContext)
  def setLoadCsvPeriodicCommitObserver(batchRowCount: Long)
  def setPipeDecorator(newDecorator: PipeDecorator)
  def setExceptionDecorator(newDecorator: CypherException => CypherException)
  def build(planType: ExecutionMode,
            params: MapValue,
            notificationLogger: InternalNotificationLogger,
            plannerName: PlannerName,
            runtimeName: RuntimeName,
            readOnly: Boolean,
            cardinalities: Cardinalities): InternalExecutionResult
}

trait ExecutionResultBuilderFactory {
  def create(): ExecutionResultBuilder
}

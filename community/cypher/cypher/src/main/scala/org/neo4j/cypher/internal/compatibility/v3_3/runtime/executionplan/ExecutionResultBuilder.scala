/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.PipeDecorator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionMode, RuntimeName}
import org.neo4j.cypher.internal.frontend.v3_3.CypherException
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.virtual.MapValue

trait ExecutionResultBuilder {
  def setQueryContext(context: QueryContext)
  def setLoadCsvPeriodicCommitObserver(batchRowCount: Long)
  def setPipeDecorator(newDecorator: PipeDecorator)
  def setExceptionDecorator(newDecorator: CypherException => CypherException)
  def build(queryId: AnyRef, planType: ExecutionMode, params: MapValue,
            notificationLogger: InternalNotificationLogger, runtimeName: RuntimeName): InternalExecutionResult
}

trait ExecutionResultBuilderFactory {
  def create(): ExecutionResultBuilder
}

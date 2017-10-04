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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.ValueConversion
import org.neo4j.cypher.internal.spi.v3_4.TransactionalContextWrapper
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.{MapValue, VirtualValues}

case class PreparedPlanExecution(plan: ExecutionPlan, executionMode: CypherExecutionMode, extractedParams: Map[String, Any]) {
  def execute(transactionalContext: TransactionalContextWrapper, params: MapValue): Result =
    plan.run(transactionalContext, executionMode, VirtualValues.combine(params,ValueConversion.asValues(extractedParams)))

  def profile(transactionalContext: TransactionalContextWrapper, params: MapValue): Result =
    plan.run(transactionalContext, CypherExecutionMode.profile, VirtualValues.combine(params,ValueConversion.asValues(extractedParams)))
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal.compiler.v3_4.CacheCheckResult
import org.neo4j.cypher.internal.runtime.interpreted.{LastCommittedTxIdProvider, TransactionalContextWrapper}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.values.virtual.MapValue

trait ExecutionPlan {

  def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode, params: MapValue): Result

  def isPeriodicCommit: Boolean

  def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): CacheCheckResult

  // This is to force eager calculation
  val plannerInfo: PlannerInfo
}

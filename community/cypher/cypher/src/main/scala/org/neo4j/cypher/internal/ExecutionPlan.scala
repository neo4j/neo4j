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

import org.neo4j.cypher.internal.spi.v3_3.TransactionalContextWrapper
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.query.PlannerInfo

final case class TransactionInfo(tx: Transaction, isTopLevelTx: Boolean, statement: Statement)

trait ExecutionPlan {

  def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult

  def isPeriodicCommit: Boolean

  def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): Boolean

  def plannerInfo: PlannerInfo
}

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
package org.neo4j.cypher.internal.spi.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.spi.QueryTransactionalContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Revertable
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.api.txstate.TxStateHolder
import org.neo4j.kernel.api.{KernelTransaction, ResourceTracker, Statement}
import org.neo4j.kernel.impl.query.TransactionalContext

case class TransactionalContextWrapper(tc: TransactionalContext) extends QueryTransactionalContext {

  override type ReadOps = Nothing

  override type DbmsOps = DbmsOperations

  def getOrBeginNewIfClosed(): TransactionalContextWrapper = TransactionalContextWrapper(tc.getOrBeginNewIfClosed())

  def isOpen: Boolean = tc.isOpen

  def graph: GraphDatabaseQueryService = tc.graph()

  def statement: Statement = tc.statement()

  def kernelTransaction: KernelTransaction = tc.kernelTransaction()

  def stateView: TxStateHolder = tc.stateView()

  override def readOperations: Nothing = ???

  override def dbmsOperations: DbmsOperations = tc.dbmsOperations()

  override def commitAndRestartTx() { tc.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = tc.isTopLevelTx

  override def close(success: Boolean) { tc.close(success) }

  def restrictCurrentTransaction(context: SecurityContext): Revertable = tc.restrictCurrentTransaction(context)

  def securityContext: SecurityContext = tc.securityContext

  def resourceTracker: ResourceTracker = tc.resourceTracker
}

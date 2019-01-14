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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.planner.v3_4.spi.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.graphdb.{Lock, PropertyContainer}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Revertable
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.api.txstate.TxStateHolder
import org.neo4j.kernel.api.{KernelTransaction, ResourceTracker, Statement}
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.query.TransactionalContext

case class TransactionalContextWrapper(tc: TransactionalContext) extends QueryTransactionalContext {
  def twoLayerTransactionState: Boolean = tc.twoLayerTransactionState()

  def getOrBeginNewIfClosed(): TransactionalContextWrapper = TransactionalContextWrapper(tc.getOrBeginNewIfClosed())

  def isOpen: Boolean = tc.isOpen

  def kernelTransaction: KernelTransaction = tc.kernelTransaction()

  def graph: GraphDatabaseQueryService = tc.graph()

  def statement: Statement = tc.statement()

  def stateView: TxStateHolder = tc.stateView()

  def cleanForReuse(): Unit = tc.cleanForReuse()

  // needed only for compatibility with 2.3
  def acquireWriteLock(p: PropertyContainer): Lock = tc.acquireWriteLock(p)


  override def cursors: CursorFactory = tc.kernelTransaction.cursors()

  override def dataRead: Read = tc.kernelTransaction().dataRead()

  override def stableDataRead: Read = tc.kernelTransaction().stableDataRead()

  override def markAsStable(): Unit = tc.kernelTransaction().markAsStable()

  override def tokenRead: TokenRead = tc.kernelTransaction().tokenRead()

  override def schemaRead: SchemaRead = tc.kernelTransaction().schemaRead()

  override def dataWrite: Write = tc.kernelTransaction().dataWrite()

  override def dbmsOperations: DbmsOperations = tc.dbmsOperations()

  override def commitAndRestartTx() { tc.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = tc.isTopLevelTx

  override def close(success: Boolean) { tc.close(success) }

  def restrictCurrentTransaction(context: SecurityContext): Revertable = tc.restrictCurrentTransaction(context)

  def securityContext: SecurityContext = tc.securityContext

  def notifyPlanningCompleted(plannerInfo: PlannerInfo): Unit = tc.executingQuery().planningCompleted(plannerInfo)

  def kernelStatisticProvider: KernelStatisticProvider = new ProfileKernelStatisticProvider(tc.kernelStatisticProvider())

  override def databaseInfo: DatabaseInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DatabaseInfo])

  def resourceTracker: ResourceTracker = tc.resourceTracker
}

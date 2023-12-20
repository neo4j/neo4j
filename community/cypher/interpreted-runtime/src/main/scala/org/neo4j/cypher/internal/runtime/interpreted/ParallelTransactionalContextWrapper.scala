/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.graphdb.Entity
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.Locks
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.QueryContext
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.SchemaWrite
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.ExecutionContext
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.memory.MemoryTracker

class ParallelTransactionalContextWrapper(private[this] val tc: TransactionalContext,
                                          private[this] val threadSafeCursors: CursorFactory) extends TransactionalContextWrapper {
  // TODO: Make parallel transaction use safe.
  //       We want all methods going through kernelExecutionContext when it is supported instead of through tc.kernelTransaction, which is not thread-safe
  private[this] val kernelExecutionContext: ExecutionContext = tc.kernelTransaction.createExecutionContext()

  override def createKernelExecutionContext(): ExecutionContext = tc.kernelTransaction.createExecutionContext()

  override def commitTransaction(): Unit = unsupported

  override def kernelQueryContext: QueryContext = tc.kernelTransaction.queryContext() // kernelExecutionContext.queryContext

  // TODO: We eventually want to use kernelExecutionContext.cursors() when it is supported and then we can remove threadSafeCursors
  override def cursors: CursorFactory = threadSafeCursors // kernelExecutionContext.cursors()

  override def cursorContext: CursorContext = kernelExecutionContext.cursorContext

  override def memoryTracker: MemoryTracker = tc.kernelTransaction.memoryTracker() // kernelExecutionContext.memoryTracker()

  override def locks: Locks = tc.kernelTransaction.locks() // kernelExecutionContext.locks()

  override def dataRead: Read = tc.kernelTransaction.dataRead() // kernelExecutionContext.dataRead()

  override def dataWrite: Write = unsupported()

  override def tokenRead: TokenRead = tc.kernelTransaction.tokenRead() // kernelExecutionContext.tokenRead()

  override def tokenWrite: TokenWrite = unsupported()

  override def token: Token = unsupported()

  override def schemaRead: SchemaRead = unsupported()

  override def schemaWrite: SchemaWrite = unsupported()

  override def procedures: Procedures = tc.kernelTransaction.procedures() // kernelExecutionContext.procedures()

  override def securityContext: SecurityContext = tc.kernelTransaction.securityContext() // kernelExecutionContext.securityContext()

  override def securityAuthorizationHandler: SecurityAuthorizationHandler = tc.kernelTransaction.securityAuthorizationHandler() // kernelExecutionContext.securityAuthorizationHandler()

  override def assertTransactionOpen(): Unit = tc.kernelTransaction.assertOpen()

  override def commitAndRestartTx(): Unit = unsupported()

  override def isTopLevelTx: Boolean = tc.isTopLevelTx

  override def close(): Unit = {
    kernelExecutionContext.complete()
    kernelExecutionContext.close()
    // tc and threadSafeCursors needs to be closed by external owner
  }

  override def kernelStatisticProvider: KernelStatisticProvider = ProfileKernelStatisticProvider(tc.kernelStatisticProvider())

  override def dbmsInfo: DbmsInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DbmsInfo])

  override def databaseId: NamedDatabaseId = tc.databaseId()

  override def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T = unsupported()

  override def rollback(): Unit = unsupported()

  override def contextWithNewTransaction: ParallelTransactionalContextWrapper = unsupported()

  override def freezeLocks(): Unit = tc.kernelTransaction.freezeLocks()

  override def thawLocks(): Unit = tc.kernelTransaction.thawLocks()

  override def validateSameDB[E <: Entity](entity: E): E = tc.transaction().validateSameDB(entity)

  override def kernelTransaction: KernelTransaction = unsupported

  // TODO: Make parallel transaction use safe. We do not want to support this in parallel, since it exposes non-thread safe APIs
  override def kernelTransactionalContext: TransactionalContext = tc

  override def graph: GraphDatabaseQueryService = unsupported

  private def unsupported(): Nothing = {
    throw new UnsupportedOperationException("Not supported in parallel runtime.")
  }

  override def createParallelTransactionalContext(): ParallelTransactionalContextWrapper = {
    require(threadSafeCursors != null)
    new ParallelTransactionalContextWrapper(kernelTransactionalContext, threadSafeCursors)
  }

  override def cancellationChecker: CancellationChecker = new TransactionCancellationChecker(kernelTransaction)
}


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
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
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

/**
 * @param threadSafeCursors use this instead of the cursors of the current transaction, unless this is `null`.
 */
abstract class TransactionalContextWrapper extends QueryTransactionalContext {

  def kernelTransaction: KernelTransaction

  def kernelTransactionalContext: TransactionalContext

  def graph: GraphDatabaseQueryService

  def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T

  def contextWithNewTransaction: TransactionalContextWrapper

  def createParallelTransactionalContext(): ParallelTransactionalContextWrapper

  def cancellationChecker: CancellationChecker
}

class SingleThreadedTransactionalContextWrapper(tc: TransactionalContext, threadSafeCursors: CursorFactory = null) extends TransactionalContextWrapper {

  override def kernelTransaction: KernelTransaction = tc.kernelTransaction()

  override def kernelTransactionalContext: TransactionalContext = tc

  override def graph: GraphDatabaseQueryService = tc.graph()

  override def createKernelExecutionContext(): ExecutionContext = tc.kernelTransaction.createExecutionContext()

  override def commitTransaction(): Unit = tc.transaction.commit()

  override def kernelQueryContext: QueryContext = tc.kernelTransaction.queryContext

  override def cursors: CursorFactory = if (threadSafeCursors == null) tc.kernelTransaction.cursors() else threadSafeCursors

  override def cursorContext: CursorContext = tc.kernelTransaction.cursorContext

  override def memoryTracker: MemoryTracker = tc.kernelTransaction().memoryTracker()

  override def locks: Locks = tc.kernelTransaction().locks()

  override def dataRead: Read = tc.kernelTransaction().dataRead()

  override def dataWrite: Write = tc.kernelTransaction().dataWrite()

  override def tokenRead: TokenRead = tc.kernelTransaction().tokenRead()

  override def tokenWrite: TokenWrite = tc.kernelTransaction().tokenWrite()

  override def token: Token = tc.kernelTransaction().token()

  override def schemaRead: SchemaRead = tc.kernelTransaction().schemaRead()

  override def schemaWrite: SchemaWrite = tc.kernelTransaction().schemaWrite()

  override def procedures: Procedures = tc.kernelTransaction.procedures()

  override def securityContext: SecurityContext = tc.kernelTransaction.securityContext

  override def securityAuthorizationHandler: SecurityAuthorizationHandler = tc.kernelTransaction.securityAuthorizationHandler()

  override def assertTransactionOpen(): Unit = tc.kernelTransaction.assertOpen()

  override def commitAndRestartTx(): Unit = tc.commitAndRestartTx()

  override def isTopLevelTx: Boolean = tc.isTopLevelTx

  override def close(): Unit = tc.close()

  override def kernelStatisticProvider: KernelStatisticProvider = ProfileKernelStatisticProvider(tc.kernelStatisticProvider())

  override def dbmsInfo: DbmsInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DbmsInfo])

  override def databaseId: NamedDatabaseId = tc.databaseId()

  def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T = {
    val javaCreator = new java.util.function.Function[SchemaStateKey, T]() {
      def apply(key: SchemaStateKey): T = f
    }
    schemaRead.schemaStateGetOrCreate(key, javaCreator)
  }

  override def rollback(): Unit = tc.rollback()

  override def contextWithNewTransaction: TransactionalContextWrapper = {
    if (threadSafeCursors != null) {
      throw new UnsupportedOperationException("Cypher transactions are not designed to work with parallel runtime, yet.")
    }
    val newTC = tc.contextWithNewTransaction()
    TransactionalContextWrapper(newTC, threadSafeCursors)
  }

  override def freezeLocks(): Unit = tc.kernelTransaction.freezeLocks()

  override def thawLocks(): Unit = tc.kernelTransaction.thawLocks()

  override def validateSameDB[E <: Entity](entity: E): E = tc.transaction().validateSameDB(entity)

  override def createParallelTransactionalContext(): ParallelTransactionalContextWrapper = {
    require(threadSafeCursors != null)
    new ParallelTransactionalContextWrapper(kernelTransactionalContext, threadSafeCursors)
  }

  override val cancellationChecker: CancellationChecker = new TransactionCancellationChecker(kernelTransaction)
}

object TransactionalContextWrapper {
  def apply(tc: TransactionalContext, threadSafeCursors: CursorFactory = null): TransactionalContextWrapper = {
    new SingleThreadedTransactionalContextWrapper(tc, threadSafeCursors)
  }
}
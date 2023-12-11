/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.configuration.Config
import org.neo4j.csv.reader.CharReadable
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.TransactionId
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
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.ExecutionContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.statistic.StatisticProvider
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.ElementIdMapper
import org.neo4j.values.ValueMapper

import java.net.URL

abstract class TransactionalContextWrapper extends QueryTransactionalContext {

  def kernelTransaction: KernelTransaction

  def kernelTransactionalContext: TransactionalContext

  def graph: GraphDatabaseQueryService

  def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T

  def contextWithNewTransaction: TransactionalContextWrapper

  def createParallelTransactionalContext(): ParallelTransactionalContextWrapper

  def cancellationChecker: CancellationChecker

  def getImportDataConnection(url: URL): CharReadable
}

class SingleThreadedTransactionalContextWrapper(tc: TransactionalContext)
    extends TransactionalContextWrapper {

  override def kernelTransaction: KernelTransaction = tc.kernelTransaction()

  override def kernelTransactionalContext: TransactionalContext = tc

  override def graph: GraphDatabaseQueryService = tc.graph()

  override def commitTransaction(): Unit = tc.commit()

  override def kernelQueryContext: QueryContext = tc.kernelTransaction.queryContext

  override def cursors: CursorFactory = tc.kernelTransaction.cursors()

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

  override def securityAuthorizationHandler: SecurityAuthorizationHandler =
    tc.kernelTransaction.securityAuthorizationHandler()

  override def accessMode: AccessMode = tc.kernelTransaction.securityContext.mode

  override def isTransactionOpen: Boolean = tc.kernelTransaction.isOpen && !tc.kernelTransaction.isTerminated

  override def assertTransactionOpen(): Unit = tc.kernelTransaction.assertOpen()

  override def close(): Unit = {
    if (DebugSupport.DEBUG_TRANSACTIONAL_CONTEXT) {
      DebugSupport.TRANSACTIONAL_CONTEXT.log(
        "%s.close(): %s thread=%s",
        this.getClass.getSimpleName,
        this,
        Thread.currentThread().getName
      )
    }
    tc.close()
  }

  override def kernelStatisticProvider: StatisticProvider = tc.kernelStatisticProvider()

  override def dbmsInfo: DbmsInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DbmsInfo])

  override def databaseId: NamedDatabaseId = tc.databaseId()

  def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T = {
    val javaCreator = new java.util.function.Function[SchemaStateKey, T]() {
      def apply(key: SchemaStateKey): T = f
    }
    schemaRead.schemaStateGetOrCreate(key, javaCreator)
  }

  override def rollback(): Unit = tc.rollback()

  override def markForTermination(reason: Status): Unit = kernelTransaction.markForTermination(reason)

  override def contextWithNewTransaction: TransactionalContextWrapper = {
    val newTC = tc.contextWithNewTransaction()
    TransactionalContextWrapper(newTC)
  }

  override def validateSameDB[E <: Entity](entity: E): Unit = tc.transaction().validateSameDB(entity)

  override def createParallelTransactionalContext(): ParallelTransactionalContextWrapper = {
    val parallelContext = new ParallelTransactionalContextWrapper(kernelTransactionalContext)
    if (DebugSupport.DEBUG_TRANSACTIONAL_CONTEXT) {
      DebugSupport.TRANSACTIONAL_CONTEXT.log(
        "%s.createParallelTransactionalContext(): %s thread=%s",
        this.getClass.getSimpleName,
        parallelContext,
        Thread.currentThread().getName
      )
    }
    parallelContext
  }

  override def elementIdMapper(): ElementIdMapper = tc.elementIdMapper()

  override val cancellationChecker: CancellationChecker = new TransactionCancellationChecker(kernelTransaction)

  override def getImportDataConnection(url: URL): CharReadable = tc.graph().validateURLAccess(securityContext, url)

  override def userTransactionId: String = {
    TransactionId(tc.databaseId().name(), tc.kernelTransaction().getTransactionSequenceNumber).toString
  }

  override def config: Config = {
    tc.graph().getDependencyResolver.resolveDependency(classOf[Config])
  }

  override def kernelExecutingQuery: org.neo4j.kernel.api.query.ExecutingQuery = {
    tc.executingQuery()
  }

  override def kernelExecutionContext: ExecutionContext =
    throw new UnsupportedOperationException("operation only possible in parallel runtime")

  override def createValueMapper: ValueMapper[AnyRef] = {
    new DefaultValueMapper(tc.transaction())
  }
}

object TransactionalContextWrapper {

  def apply(tc: TransactionalContext): TransactionalContextWrapper = {
    new SingleThreadedTransactionalContextWrapper(tc)
  }
}

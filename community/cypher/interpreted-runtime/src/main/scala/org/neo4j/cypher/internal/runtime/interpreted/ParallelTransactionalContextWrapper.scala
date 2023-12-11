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
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.ExecutionContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.api.parallel.ExecutionContextValueMapper
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.statistic.StatisticProvider
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.ElementIdMapper
import org.neo4j.values.ValueMapper

import java.net.URL

class ParallelTransactionalContextWrapper(
  private[this] val tc: TransactionalContext
) extends TransactionalContextWrapper {

  // NOTE: We want all methods going through kernelExecutionContext instead of through tc.kernelTransaction, which is not thread-safe
  private[this] val _kernelExecutionContext: ExecutionContext = {
    val ktx = tc.kernelTransaction()
    ktx.assertOpen()
    ktx.createExecutionContext()
  }

  private[this] val _statisticsProvider = new StatisticProvider {
    private val tracer: PageCursorTracer = _kernelExecutionContext.cursorContext().getCursorTracer

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults();
  }

  override def kernelExecutionContext: ExecutionContext = _kernelExecutionContext

  override def commitTransaction(): Unit = unsupported()

  override def kernelQueryContext: QueryContext = _kernelExecutionContext.queryContext

  override def cursors: CursorFactory = _kernelExecutionContext.cursors()

  override def cursorContext: CursorContext = _kernelExecutionContext.cursorContext

  override def memoryTracker: MemoryTracker = kernelExecutionContext.memoryTracker()

  override def locks: Locks = _kernelExecutionContext.locks()

  override def dataRead: Read = _kernelExecutionContext.dataRead()

  override def dataWrite: Write = unsupported()

  override def tokenRead: TokenRead = _kernelExecutionContext.tokenRead()

  override def tokenWrite: TokenWrite = unsupported()

  override def token: Token = unsupported()

  override def schemaRead: SchemaRead = unsupported()

  override def schemaWrite: SchemaWrite = unsupported()

  override def procedures: Procedures = _kernelExecutionContext.procedures()

  override def securityContext: SecurityContext = _kernelExecutionContext.securityContext()

  override def securityAuthorizationHandler: SecurityAuthorizationHandler =
    _kernelExecutionContext.securityAuthorizationHandler()

  override def accessMode: AccessMode = _kernelExecutionContext.securityContext().mode()

  override def isTransactionOpen: Boolean = _kernelExecutionContext.isTransactionOpen

  override def assertTransactionOpen(): Unit = _kernelExecutionContext.performCheckBeforeOperation()

  override def close(): Unit = {
    if (DebugSupport.DEBUG_TRANSACTIONAL_CONTEXT) {
      DebugSupport.TRANSACTIONAL_CONTEXT.log(
        "%s.close(): %s thread=%s",
        this.getClass.getSimpleName,
        this,
        Thread.currentThread().getName
      )
    }
    _kernelExecutionContext.complete()
    _kernelExecutionContext.close()
  }

  override def kernelStatisticProvider: StatisticProvider = _statisticsProvider
  override def dbmsInfo: DbmsInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DbmsInfo])

  override def databaseId: NamedDatabaseId = tc.databaseId()

  override def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T = unsupported()

  override def rollback(): Unit = unsupported()

  override def markForTermination(reason: Status): Unit = unsupported()

  override def contextWithNewTransaction: ParallelTransactionalContextWrapper = unsupported()

  override def validateSameDB[E <: Entity](entity: E): Unit = tc.transaction().validateSameDB(entity)

  override def kernelTransaction: KernelTransaction = unsupported()

  override def kernelTransactionalContext: TransactionalContext = unsupported()

  override def graph: GraphDatabaseQueryService = unsupported()

  private def unsupported(): Nothing = {
    throw new UnsupportedOperationException("Not supported in parallel runtime.")
  }

  override def createParallelTransactionalContext(): ParallelTransactionalContextWrapper = {
    new ParallelTransactionalContextWrapper(kernelTransactionalContext)
  }

  override def elementIdMapper(): ElementIdMapper = tc.elementIdMapper()

  override def cancellationChecker: CancellationChecker = new TransactionCancellationChecker(kernelTransaction)

  override def getImportDataConnection(url: URL): CharReadable = tc.graph().validateURLAccess(securityContext, url)

  override def userTransactionId: String = unsupported()

  override def config: Config = {
    tc.graph().getDependencyResolver.resolveDependency(classOf[Config])
  }

  override def kernelExecutingQuery: org.neo4j.kernel.api.query.ExecutingQuery = {
    tc.executingQuery()
  }

  override def createValueMapper: ValueMapper[AnyRef] = {
    new ExecutionContextValueMapper(_kernelExecutionContext)
  }
}

/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api._
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.query.TransactionalContext

/**
  * TODO: Currently threadSafeCursors is entirely unused (always null), so we should consider removing it
  *
  * @param threadSafeCursors use this instead of the cursors of the current transaction, unless this is `null`.
  */
case class TransactionalContextWrapper(tc: TransactionalContext, threadSafeCursors: CursorFactory = null) extends QueryTransactionalContext {

  def kernelTransaction: KernelTransaction = tc.kernelTransaction()

  def graph: GraphDatabaseQueryService = tc.graph()

  override def transaction: KernelTransaction = tc.kernelTransaction

  override def cursors: CursorFactory = if (threadSafeCursors == null) tc.kernelTransaction.cursors() else threadSafeCursors

  override def dataRead: Read = tc.kernelTransaction().dataRead()

  override def tokenRead: TokenRead = tc.kernelTransaction().tokenRead()

  override def schemaRead: SchemaRead = tc.kernelTransaction().schemaRead()

  override def dataWrite: Write = tc.kernelTransaction().dataWrite()

  override def dbmsOperations: DbmsOperations = tc.dbmsOperations()

  override def commitAndRestartTx() { tc.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = tc.isTopLevelTx

  override def close() { tc.close() }

  override def kernelStatisticProvider: KernelStatisticProvider = new ProfileKernelStatisticProvider(tc.kernelStatisticProvider())

  override def databaseInfo: DatabaseInfo = tc.graph().getDependencyResolver.resolveDependency(classOf[DatabaseInfo])

  override def databaseId: NamedDatabaseId = tc.databaseId()

  def getOrCreateFromSchemaState[T](key: SchemaStateKey, f: => T): T = {
    val javaCreator = new java.util.function.Function[SchemaStateKey, T]() {
      def apply(key: SchemaStateKey) = f
    }
    schemaRead.schemaStateGetOrCreate(key, javaCreator)
  }

  override def rollback(): Unit = tc.rollback()
}

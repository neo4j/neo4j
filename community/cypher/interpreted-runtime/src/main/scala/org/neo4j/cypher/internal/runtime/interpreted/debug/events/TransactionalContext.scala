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
package org.neo4j.cypher.internal.runtime.interpreted.debug.events

import org.neo4j.cypher.internal.runtime.debug.events.DebugCategory
import org.neo4j.cypher.internal.runtime.interpreted.ParallelTransactionalContextWrapper

enum TransactionalContext extends DebugCategory.TransactionalContext {

  case BeginTransaction(scopeId: String)

  case CommitTransaction(scopeId: String)

  case RollbackTransaction(scopeId: String)

  case OnErrorInInnerTransaction(cause: Throwable)

  case Close

  case CloseWithSelf(self: Any)

  case CloseQueryStateForWorker(self: Class[_], workerId: Int)

  case CreateParallelContext(self: Class[_], parallelContext: ParallelTransactionalContextWrapper)

  case BeginBatch(scopeId: String, batchId: Long)

  case EndBatch(scopeId: String, batchId: Long)

  case RetryBatch(batchId: Long, lastBatch: Boolean)
}
